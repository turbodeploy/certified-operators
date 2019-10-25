package com.vmturbo.history.stats.writers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.stats.ICompleteTopologyStatsWriter;
import com.vmturbo.history.utils.HostsSumCapacities;
import com.vmturbo.history.utils.StoragesSumCapacities;
import com.vmturbo.history.utils.SystemLoadCommodities;
import com.vmturbo.history.utils.SystemLoadHelper;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * The class SystemLoadSnapshot saves information about the current shapshot of the system, using
 * the commodities participating in the calculation of system load. More specifically:
 * For each VM:
 *      -For the sold commodities VCPU and VMEM it saves their cluster id, VM id, used values and
 *          capacities.
 *      -For the commodities CPU, MEM, IO_THROUGHPUT, NET_THROUGHPUT, CPU_PROVISIONED
 *          and MEM_PROVISIONED bought from a host it saves their cluster id, VM id, producer (host)
 *          id and used values.
 *      -For the commodities STORAGE_ACCESS, STORAGE_PROVISIONED and STORAGE_AMOUNT bought from a
 *          storage it saves their cluster id, VM id, producer (storage) id and used values.
 *      -For all these 11 commodities it saves the sum of used values and capacities per slice.
 */
public class SystemLoadSnapshot extends AbstractStatsWriter implements ICompleteTopologyStatsWriter {

    private final Logger logger = LogManager.getLogger();

    private final GroupServiceBlockingStub groupServiceClient;
    private final SystemLoadHelper systemLoadHelper;


    // Tables to keep statistics for system load
    private Map<String, Double[]> slice2sumUsed = null;
    private Map<String, Double[]> slice2sumCapacities = null;
    private Multimap<String, Pair<CommoditySoldDTO, TopologyEntityDTO>> updatedSoldCommodities = null;
    private Multimap<String, Pair<CommodityBoughtDTO, TopologyEntityDTO>> updatedBoughtCommodities = null;

    /**
     * Creates {@link SystemLoadSnapshot} instance.
     *
     * @param groupServiceClient A client to request groups info from group
     *                 component.
     * @param systemLoadHelper helps to update and save system load data.
     */
    public SystemLoadSnapshot(@Nonnull GroupServiceBlockingStub groupServiceClient,
                    @Nonnull SystemLoadHelper systemLoadHelper) {
        this.groupServiceClient = groupServiceClient;
        this.systemLoadHelper = systemLoadHelper;
    }

    /**
     * The method saves information about the current system load commodities of
     * the system. This info is the commodities per slice and the sums of used values and
     * capacities per system load commodity per slice.
     *
     * @param allTopologyDTOs All the topology entities of the current snapshot.
     */
    @Override
    protected int process(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> allTopologyDTOs) {

        final Map<String, Grouping> slice2groups = new HashMap<>();
        boolean savedData = false;

        // Initialize the tables to keep statistics for system load
        slice2sumUsed = Maps.newHashMap();
        slice2sumCapacities = Maps.newHashMap();
        updatedSoldCommodities = HashMultimap.create();
        updatedBoughtCommodities = HashMultimap.create();

        // Getting information for the groups of the system
        GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder()
                                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER))
                        .build();
        Iterator<Grouping> groupIterator = groupServiceClient.getGroups(groupsRequest);

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

        systemLoadHelper.setSliceToGroups(slice2groups);
        HostsSumCapacities.init();
        StoragesSumCapacities.init();

        // Iterating over all commodities and updating the usage info per slice for the
        // system load commodities.
        for (TopologyEntityDTO topologyEntity : allTopologyDTOs) {
            if (topologyEntity.getEntityType() == EntityType.VIRTUAL_MACHINE.ordinal()) {
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
            } else if (topologyEntity.getEntityType() == EntityType.PHYSICAL_MACHINE.ordinal()) {
                List<String> slices = systemLoadHelper.getSlices(topologyEntity);
                if (CollectionUtils.isEmpty(slices)) {
                    logger.debug("Unable to find slice for entity " + topologyEntity);
                } else {
                    for (CommoditySoldDTO commSold: topologyEntity.getCommoditySoldListList()) {
                        if (SystemLoadCommodities.isHostCommodity(commSold.getCommodityType().getType())) {
                            switch (SystemLoadCommodities.toSystemLoadCommodity(commSold.getCommodityType().getType())) {
                                case CPU:
                                    for (String slice : slices) {
                                        Map<String, Double> cpuSums = HostsSumCapacities.getCpu();
                                        cpuSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                        HostsSumCapacities.setCpu(cpuSums);
                                    }
                                    break;
                                case MEM:
                                    for (String slice : slices) {
                                        Map<String, Double> memSums = HostsSumCapacities.getMem();
                                        memSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                        HostsSumCapacities.setMem(memSums);
                                    }
                                    break;
                                case IO_THROUGHPUT:
                                    for (String slice : slices) {
                                        Map<String, Double> ioThroughputSums = HostsSumCapacities.getIoThroughput();
                                        ioThroughputSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                        HostsSumCapacities.setIoThroughput(ioThroughputSums);
                                    }
                                    break;
                                case NET_THROUGHPUT:
                                    for (String slice : slices) {
                                        Map<String, Double> netThroughputSums = HostsSumCapacities.getNetThroughput();
                                        netThroughputSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                        HostsSumCapacities.setNetThroughput(netThroughputSums);
                                    }
                                    break;
                                case CPU_PROVISIONED:
                                    for (String slice : slices) {
                                        Map<String, Double> cpuProvisionedSums = HostsSumCapacities.getCpuProvisioned();
                                        cpuProvisionedSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                        HostsSumCapacities.setCpuProvisioned(cpuProvisionedSums);
                                    }
                                    break;
                                case MEM_PROVISIONED:
                                    for (String slice : slices) {
                                        Map<String, Double> memProvisionedSums = HostsSumCapacities.getMemProvisioned();
                                        memProvisionedSums.merge(slice, commSold.getCapacity(), (s1, s2) -> s1 + s2);
                                        HostsSumCapacities.setMemProvisioned(memProvisionedSums);
                                    }
                                    break;
                                default:
                                    logger.warn("Skipping system load commodity sold : " + commSold
                                                    + " by host : " + topologyEntity);
                                    break;
                            }
                        }
                    }
                }
            } else if (topologyEntity.getEntityType() == EntityType.STORAGE.ordinal()) {
                List<String> slices = systemLoadHelper.getSlices(topologyEntity);
                if (CollectionUtils.isEmpty(slices)) {
                    logger.debug("Unable to find slice for entity " + topologyEntity);
                } else {
                    for (CommoditySoldDTO commSold: topologyEntity.getCommoditySoldListList()) {
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

        }

        systemLoadHelper.updateCapacities(slice2sumCapacities);

        // Updating system load info in DB
        if (savedData) {
            systemLoadHelper.updateSystemLoad(Maps.newHashMap(slice2sumUsed),
                    Maps.newHashMap(slice2sumCapacities),
                    ArrayListMultimap.create(updatedSoldCommodities),
                    ArrayListMultimap.create(updatedBoughtCommodities));
        }
        return allTopologyDTOs.size();
    }

    /**
     * The method updates the sums of used values and capacities for each commodity sold
     * by a VM and adds this commodity to the commodities of the slice.
     *
     * <p>N.B> Reader must ensure that commodity is of a type appearing in
     * {@link SystemLoadCommodities}.</p>
     *
     * @param slice The slice that will be updated.
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
     * @param slice The slice that will be updated.
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

}
