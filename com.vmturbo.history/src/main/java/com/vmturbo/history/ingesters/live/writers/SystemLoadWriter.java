package com.vmturbo.history.ingesters.live.writers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.Pair;
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
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.abstraction.tables.SystemLoad;
import com.vmturbo.history.schema.abstraction.tables.records.SystemLoadRecord;
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

    // Tables to keep statistics for system load
    private Map<String, Double[]> slice2sumUsed = new HashMap();
    private Map<String, Double[]> slice2sumCapacities = new HashMap<>();
    private Multimap<String, Pair<CommoditySoldDTO, TopologyEntityDTO>> updatedSoldCommodities = HashMultimap.create();
    private Multimap<String, Pair<CommodityBoughtDTO, TopologyEntityDTO>> updatedBoughtCommodities = HashMultimap.create();
    private final Map<String, Grouping> slice2groups = new HashMap<>();
    private boolean savedData = false;

    /**
     * Create a new instance.
     * @param groupServiceClient access to group service
     * @param systemLoadHelper   helper class for system load
     * @param loaders            factory for bulk loaders
     */
    public SystemLoadWriter(@Nonnull GroupServiceBlockingStub groupServiceClient,
                            @Nonnull SystemLoadHelper systemLoadHelper,
                            @Nonnull SimpleBulkLoaderFactory loaders) {
        this.groupServiceClient = groupServiceClient;
        this.systemLoadHelper = systemLoadHelper;
        this.systemLoadLoader = loaders.getLoader(SystemLoad.SYSTEM_LOAD);
        loadGroups();
        systemLoadHelper.setSliceToGroups(slice2groups);
        HostsSumCapacities.init();
        StoragesSumCapacities.init();
    }

    private void loadGroups() {
        // Getting information for the groups of the system
        GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                    .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                    .build())
            .build();
        final Iterator<Grouping> groupIterator = groupServiceClient.getGroups(groupsRequest);
        // Saving host clusters info
        while (groupIterator.hasNext()) {
            Grouping curGroup = groupIterator.next();
            long groupId = curGroup.getId();
            String slice = Long.toString(groupId);
            if (slice2groups.containsKey(slice)) {
                logger.error("A slice with id = " + slice + " already exists");
            } else {
                slice2groups.put(slice, curGroup);
            }
        }
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
        List<String> slices = systemLoadHelper.getSlices(topologyEntity);
        if (CollectionUtils.isEmpty(slices)) {
            logger.debug("Unable to find slice for entity " + topologyEntity);
        } else {
            for (CommoditySoldDTO commSold : topologyEntity.getCommoditySoldListList()) {
                if (SystemLoadCommodities.isStorageCommodity(commSold.getCommodityType().getType())) {
                    switch (SystemLoadCommodities.toSystemLoadCommodity(commSold.getCommodityType().getType())) {
                        case STORAGE_ACCESS:
                            for (String slice : slices) {
                                Map<String, Double> storageAccessSums = StoragesSumCapacities.getStorageAccess();
                                storageAccessSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                StoragesSumCapacities.setStorageAccess(storageAccessSums);
                            }
                            break;
                        case STORAGE_PROVISIONED:
                            for (String slice : slices) {
                                Map<String, Double> storageProvisionedSums = StoragesSumCapacities.getStorageProvisioned();
                                storageProvisionedSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                StoragesSumCapacities.setStorageProvisioned(storageProvisionedSums);
                            }
                            break;
                        case STORAGE_AMOUNT:
                            for (String slice : slices) {
                                Map<String, Double> storageAmountSums = StoragesSumCapacities.getStorageAmount();
                                storageAmountSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                StoragesSumCapacities.setStorageAmount(storageAmountSums);
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
        List<String> slices = systemLoadHelper.getSlices(topologyEntity);
        if (CollectionUtils.isEmpty(slices)) {
            logger.debug("Unable to find slice for entity " + topologyEntity);
        } else {
            for (CommoditySoldDTO commSold : topologyEntity.getCommoditySoldListList()) {
                if (SystemLoadCommodities.isHostCommodity(commSold.getCommodityType().getType())) {
                    processPmHostCommodity(topologyEntity, slices, commSold);
                }
            }
        }
    }

    private void processPmHostCommodity(final TopologyEntityDTO topologyEntity, final List<String> slices, final CommoditySoldDTO commSold) {
        final Supplier<Map<String, Double>> sumsGetter;
        final Consumer<Map<String, Double>> sumsSetter;
        switch (SystemLoadCommodities.toSystemLoadCommodity(commSold.getCommodityType().getType())) {
            case CPU:
                sumsGetter = HostsSumCapacities::getCpu;
                sumsSetter = HostsSumCapacities::setCpu;
                break;
            case MEM:
                sumsGetter = HostsSumCapacities::getMem;
                sumsSetter = HostsSumCapacities::setMem;
                break;
            case IO_THROUGHPUT:
                sumsGetter = HostsSumCapacities::getIoThroughput;
                sumsSetter = HostsSumCapacities::setIoThroughput;
                break;
            case NET_THROUGHPUT:
                sumsGetter = HostsSumCapacities::getNetThroughput;
                sumsSetter = HostsSumCapacities::setNetThroughput;
                break;
            case CPU_PROVISIONED:
                sumsGetter = HostsSumCapacities::getCpuProvisioned;
                sumsSetter = HostsSumCapacities::setCpuProvisioned;
                break;
            case MEM_PROVISIONED:
                sumsGetter = HostsSumCapacities::getMemProvisioned;
                sumsSetter = HostsSumCapacities::setMemProvisioned;
                break;
            default:
                logger.warn("Skipping system load commodity sold : " + commSold
                    + " by host : " + topologyEntity);
                return;
        }
        Map<String, Double> sums = sumsGetter.get();
        slices.forEach(slice -> sums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2));
        sumsSetter.accept(sums);
    }

    private void processVMEntity(final TopologyEntityDTO topologyEntity) {
        for (CommoditySoldDTO commmSold : topologyEntity.getCommoditySoldListList()) {
            if (SystemLoadCommodities.isSystemLoadCommodity(commmSold.getCommodityType().getType())) {
                List<String> slices = systemLoadHelper.getSlices(topologyEntity);
                if (CollectionUtils.isEmpty(slices)) {
                    logger.debug("Unable to find slice for the sold commodity " + commmSold);
                } else {
                    savedData = true;
                    for (String slice : slices) {
                        updateOverallSoldCommodityUsage(slice, commmSold, topologyEntity);
                    }
                }
            }
        }
        for (CommoditiesBoughtFromProvider commBoughtFromProv : topologyEntity.getCommoditiesBoughtFromProvidersList()) {
            for (CommodityBoughtDTO commBought : commBoughtFromProv.getCommodityBoughtList()) {
                if (SystemLoadCommodities.isSystemLoadCommodity(commBought.getCommodityType().getType())) {
                    List<String> slices = systemLoadHelper.getSlices(topologyEntity);
                    if (CollectionUtils.isEmpty(slices)) {
                        logger.debug("Unable to find slice for the bought commodity " + commBought);
                    } else {
                        savedData = true;
                        for (String slice : slices) {
                            updateOverallBoughtCommodityUsage(slice, commBought, topologyEntity);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void finish(int entityCount, final boolean expedite, final String infoSummary)
        throws InterruptedException {

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
        String slice, CommoditySoldDTO commodity, TopologyEntityDTO entity) {

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
        String slice, CommodityBoughtDTO commodity, TopologyEntityDTO entity) {

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
        private final SystemLoadHelper systemLoadHelper;

        /**
         * Create a new factory instance.
         * @param groupServiceClient access to group service
         * @param systemLoadHelper   system load helper
         */
        public Factory(GroupServiceBlockingStub groupServiceClient,
                       SystemLoadHelper systemLoadHelper) {
            this.groupServiceClient = groupServiceClient;
            this.systemLoadHelper = systemLoadHelper;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>>
        getChunkProcessor(final TopologyInfo topologyInfo,
                          final SimpleBulkLoaderFactory loaders) {
            return Optional.of(new SystemLoadWriter(groupServiceClient, systemLoadHelper, loaders));
        }
    }
}
