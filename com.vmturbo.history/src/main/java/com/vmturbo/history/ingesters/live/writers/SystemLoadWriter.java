package com.vmturbo.history.ingesters.live.writers;

import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.utils.HostsSumCapacities;
import com.vmturbo.history.utils.StoragesSumCapacities;
import com.vmturbo.history.utils.SystemLoadCommodities;
import com.vmturbo.history.utils.SystemLoadHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * The class SystemLoadSnapshot saves information about the current shapshot of the system, using
 * the commodities participating in the calculation of system load. More specifically:
 *
 * <p>For each VM:</p>
 *
 * <ul>
 * <li>For the sold commodities VCPU and VMEM it saves their cluster id, VM id, used values and
 * capacities.</li>
 * <li>For the commodities CPU, MEM, IO_THROUGHPUT, NET_THROUGHPUT, CPU_PROVISIONED
 * and MEM_PROVISIONED bought from a host it saves their cluster id, VM id, producer (host)
 * id and used values.</li>
 * <li>For the commodities STORAGE_ACCESS, STORAGE_PROVISIONED and STORAGE_AMOUNT bought from a
 * storage it saves their cluster id, VM id, producer (storage) id and used values.</li>
 * <li>For all these 11 commodities it saves the sum of used values and capacities per slice.</li>
 * </ul>
 */
public class SystemLoadWriter extends TopologyWriterBase {

    private final Logger logger = LogManager.getLogger();
    private final GroupServiceBlockingStub groupServiceClient;
    private final SystemLoadHelper systemLoadHelper;
    private final BulkLoader<SystemLoadRecord> systemLoadLoader;
    private final HostsSumCapacities hostsSumCapacities;
    private final StoragesSumCapacities storagesSumCapacities;

    // Tables to keep statistics for system load
    private Map<Long, Double[]> slice2sumUsed = new HashMap();
    private Map<Long, Double[]> slice2sumCapacities = new HashMap<>();
    private Multimap<Long, Pair<CommoditySoldDTO, TopologyEntityDTO>> updatedSoldCommodities
            = HashMultimap.create();
    private Multimap<Long, Pair<CommodityBoughtDTO, TopologyEntityDTO>> updatedBoughtCommodities
            = HashMultimap.create();
    private boolean savedData = false;

    /**
     * Create a new instance.
     *
     * @param groupServiceClient access to group service
     * @param systemLoadReader   system load reader instance
     * @param loaders            factory for bulk loaders
     * @param historydbIO        historydbio instance
     */
    public SystemLoadWriter(@Nonnull GroupServiceBlockingStub groupServiceClient,
            @Nonnull SystemLoadReader systemLoadReader,
            @Nonnull SimpleBulkLoaderFactory loaders,
            @Nonnull HistorydbIO historydbIO) {
        this.groupServiceClient = groupServiceClient;
        this.systemLoadLoader = loaders.getLoader(SystemLoad.SYSTEM_LOAD);
        this.hostsSumCapacities = new HostsSumCapacities();
        this.storagesSumCapacities = new StoragesSumCapacities();
        this.systemLoadHelper = new SystemLoadHelper(
                systemLoadReader, hostsSumCapacities, storagesSumCapacities, historydbIO);
        systemLoadHelper.setHostToCluster(loadClusters());
    }

    private TLongLongMap loadClusters() {
        final TLongLongMap result = new TLongLongHashMap(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, -1L);
        // Getting information for the clusters of the system
        GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                        .build())
                .build();
        final Iterator<Grouping> groupIterator = groupServiceClient.getGroups(groupsRequest);
        // Saving host clusters info
        while (groupIterator.hasNext()) {
            Grouping cluster = groupIterator.next();
            long clusterId = cluster.getId();
            GroupProtoUtil.getStaticMembers(cluster)
                    .forEach(memberId -> result.put(memberId, clusterId));
        }
        return result;
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull Collection<TopologyEntityDTO> entities,
            @Nonnull String infoSummary) {
        // Iterating over all commodities and updating the usage info per slice for the
        // system load commodities.
        entities.forEach(topologyEntity -> {
            if (topologyEntity.getEntityType() == EntityType.VIRTUAL_MACHINE.ordinal()) {
                processVMEntity(topologyEntity);
            } else if (topologyEntity.getEntityType() == EntityType.PHYSICAL_MACHINE.ordinal()) {
                processPMEntity(topologyEntity);
            } else if (topologyEntity.getEntityType() == EntityType.STORAGE.ordinal()) {
                processStorageEntity(topologyEntity);
            }
        });
        return ChunkDisposition.SUCCESS;
    }

    private void processStorageEntity(final TopologyEntityDTO topologyEntity) {
        Set<Long> slices = systemLoadHelper.getSlices(topologyEntity);
        if (slices.isEmpty()) {
            logger.debug("Unable to find slice for entity " + topologyEntity);
        } else {
            for (CommoditySoldDTO commSold : topologyEntity.getCommoditySoldListList()) {
                if (SystemLoadCommodities.isStorageCommodity(commSold.getCommodityType().getType())) {
                    final double capacity = commSold.getCapacity();
                    switch (SystemLoadCommodities.toSystemLoadCommodity(commSold.getCommodityType().getType())) {
                        case STORAGE_ACCESS:
                            for (long slice : slices) {
                                storagesSumCapacities.getStorageAccess()
                                        .adjustOrPutValue(slice, capacity, capacity);
                            }
                            break;
                        case STORAGE_PROVISIONED:
                            for (long slice : slices) {
                                storagesSumCapacities.getStorageProvisioned()
                                        .adjustOrPutValue(slice, capacity, capacity);
                            }
                            break;
                        case STORAGE_AMOUNT:
                            for (long slice : slices) {
                                storagesSumCapacities.getStorageAmount()
                                        .adjustOrPutValue(slice, capacity, capacity);
                            }
                            break;
                        default:
                            logger.warn("Skipping system load commodity sold : " + commSold
                                    + " by Storage : " + topologyEntity);
                            break;
                    }
                }
            }
        }
    }

    private void processPMEntity(final TopologyEntityDTO topologyEntity) {
        Set<Long> slices = systemLoadHelper.getSlices(topologyEntity);
        if (slices.isEmpty()) {
            logger.debug("Unable to find slice for entity " + topologyEntity);
        } else {
            for (CommoditySoldDTO commSold : topologyEntity.getCommoditySoldListList()) {
                if (SystemLoadCommodities.isHostCommodity(commSold.getCommodityType().getType())) {
                    processPmHostCommodity(topologyEntity, slices, commSold);
                }
            }
        }
    }

    private void processPmHostCommodity(final TopologyEntityDTO topologyEntity,
            final Collection<Long> slices, final CommoditySoldDTO commSold) {
        final SystemLoadCommodities sysLoadCommodity =
                SystemLoadCommodities.toSystemLoadCommodity(commSold.getCommodityType().getType());
        final double capacity = commSold.getCapacity();
        for (long slice : slices) {
            TLongDoubleMap accumulator;
            switch (sysLoadCommodity) {
                case CPU:
                    accumulator = hostsSumCapacities.getCpu();
                    break;
                case MEM:
                    accumulator = hostsSumCapacities.getMem();
                    break;
                case IO_THROUGHPUT:
                    accumulator = hostsSumCapacities.getIoThroughput();
                    break;
                case NET_THROUGHPUT:
                    accumulator = hostsSumCapacities.getNetThroughput();
                    break;
                case CPU_PROVISIONED:
                    accumulator = hostsSumCapacities.getCpuProvisioned();
                    break;
                case MEM_PROVISIONED:
                    accumulator = hostsSumCapacities.getMemProvisioned();
                    break;
                default:
                    logger.warn("Skipping system load commodity sold : " + commSold
                            + " by host : " + topologyEntity);
                    return;
            }
            accumulator.adjustOrPutValue(slice, capacity, capacity);
        }
    }

    private void processVMEntity(final TopologyEntityDTO topologyEntity) {
        Set<Long> slices = systemLoadHelper.getSlices(topologyEntity);
        if (CollectionUtils.isEmpty(slices)) {
            logger.debug("Unable to find slice for the sold commodities of {}",
                    topologyEntity.getDisplayName());
        } else {
            for (CommoditySoldDTO commmSold : topologyEntity.getCommoditySoldListList()) {
                if (SystemLoadCommodities.isSystemLoadCommodity(commmSold.getCommodityType().getType())) {
                    savedData = true;
                    for (long slice : slices) {
                        updateOverallSoldCommodityUsage(slice, commmSold, topologyEntity);
                    }
                }
            }
            for (CommoditiesBoughtFromProvider commBoughtFromProv
                    : topologyEntity.getCommoditiesBoughtFromProvidersList()) {
                for (CommodityBoughtDTO commBought : commBoughtFromProv.getCommodityBoughtList()) {
                    if (SystemLoadCommodities.isSystemLoadCommodity(commBought.getCommodityType().getType())) {
                        savedData = true;
                        for (long slice : slices) {
                            updateOverallBoughtCommodityUsage(slice, commBought, topologyEntity);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void finish(int entityCount, final boolean expedite, final String infoSummary) {

        systemLoadHelper.updateCapacities(slice2sumCapacities);

        // Updating system load info in DB
        if (savedData) {
            systemLoadHelper.updateSystemLoad(
                    systemLoadLoader,
                    Maps.newHashMap(slice2sumUsed),
                    Maps.newHashMap(slice2sumCapacities),
                    ArrayListMultimap.create(updatedSoldCommodities),
                    ArrayListMultimap.create(updatedBoughtCommodities));
        }
    }

    /**
     * The method updates the sums of used values and capacities for each commodity sold
     * by a VM and adds this commodity to the commodities of the slice.
     *
     * @param slice     The slice that will be updated.
     *
     *                  <p>N.B Reader must ensure that commodity is of a type appearing in
     *                  {@link SystemLoadCommodities}.</p>
     * @param commodity The commodity to read the used value and the capacity.
     * @param entity    the VM entity
     */
    public void updateOverallSoldCommodityUsage(
            long slice, CommoditySoldDTO commodity, TopologyEntityDTO entity) {

        SystemLoadCommodities loadComm = SystemLoadCommodities.toSystemLoadCommodity(
                commodity.getCommodityType().getType());

        // Update sums of used values for the system load commodities
        Double[] used = slice2sumUsed.get(slice);
        if (used == null) {
            used = new Double[SystemLoadCommodities.SIZE];
            slice2sumUsed.put(slice, used);
        }
        used[loadComm.ordinal()] = (used[loadComm.ordinal()] == null) ? commodity.getUsed()
                : used[loadComm.ordinal()] + commodity.getUsed();

        // Update sums of capacities for the system load commodities
        Double[] capacities = slice2sumCapacities.get(slice);
        if (capacities == null) {
            capacities = new Double[SystemLoadCommodities.SIZE];
            slice2sumCapacities.put(slice, capacities);
        }
        capacities[loadComm.ordinal()] = (capacities[loadComm.ordinal()] == null)
                ? commodity.getCapacity()
                : capacities[loadComm.ordinal()] + commodity.getCapacity();

        // Keep track of the commodities
        updatedSoldCommodities.put(slice, new Pair<>(commodity, entity));
    }

    /**
     * The method updates the sums of used values for each commodity bought
     * by a VM and adds this commodity to the commodities of the slice.
     *
     * <p>N.B. Caller must ensure that the commodity is of a type found in
     * {@link SystemLoadCommodities}.</p>
     *
     * @param slice     The slice that will be updated.
     * @param commodity The commodity to read the used value and the capacity.
     * @param entity    the VM entity
     */
    public void updateOverallBoughtCommodityUsage(
            long slice, CommodityBoughtDTO commodity, TopologyEntityDTO entity) {

        SystemLoadCommodities loadComm = SystemLoadCommodities.toSystemLoadCommodity(
                commodity.getCommodityType().getType());

        // Update sums of used values for the system load commodities
        Double[] used = slice2sumUsed.get(slice);
        if (used == null) {
            used = new Double[SystemLoadCommodities.SIZE];
            slice2sumUsed.put(slice, used);
        }
        used[loadComm.ordinal()] = (used[loadComm.ordinal()] == null) ? commodity.getUsed()
                : used[loadComm.ordinal()] + commodity.getUsed();

        // Update sums of capacities for the system load commodities
        Double[] capacities = slice2sumCapacities.get(slice);
        if (capacities == null) {
            capacities = new Double[SystemLoadCommodities.SIZE];
            slice2sumCapacities.put(slice, capacities);
        }
        capacities[loadComm.ordinal()] = 0.0;

        // Keep track of the commodities
        updatedBoughtCommodities.put(slice, new Pair<>(commodity, entity));
    }


    /**
     * Factory for SystemLoaderWriter.
     */
    public static class Factory extends TopologyWriterBase.Factory {

        private final GroupServiceBlockingStub groupServiceClient;
        private final SystemLoadReader systemLoadReader;
        private final HistorydbIO historydbIO;

        /**
         * Create a new factory instance.
         *
         * @param groupServiceClient access to group service
         * @param systemLoadReader   system load reader instance
         * @param historydbIO        hisotrydbio instance
         */
        public Factory(GroupServiceBlockingStub groupServiceClient,
                SystemLoadReader systemLoadReader, HistorydbIO historydbIO) {
            this.groupServiceClient = groupServiceClient;
            this.systemLoadReader = systemLoadReader;
            this.historydbIO = historydbIO;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>>
        getChunkProcessor(final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory loaders) {
            return Optional.of(new SystemLoadWriter(
                    groupServiceClient, systemLoadReader, loaders, historydbIO));
        }
    }
}
