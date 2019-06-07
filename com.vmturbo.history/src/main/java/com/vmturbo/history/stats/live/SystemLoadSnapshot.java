package com.vmturbo.history.stats.live;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.history.utils.HostsSumCapacities;
import com.vmturbo.history.utils.StoragesSumCapacities;
import com.vmturbo.history.utils.SystemLoadCommodities;
import com.vmturbo.history.utils.SystemLoadHelper;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * The class SystemLoadSnapshot saves information about the current shapshot of the system, using
 * the commodities participating in the calculation of system load. More specifically:
 * For each VM:
 *      -For the sold commodities VCPU, VMEM and VSTORAGE it saves their cluster id, VM id, used values
 *          and capacities.
 *      -For the commodities CPU, MEM, IO_THROUGHPUT, NET_THROUGHPUT, CPU_PROVISIONED
 *       and MEM_PROVISIONED bought from a host it saves their cluster id, VM id, producer (host) id
 *          and used values.
 *      -For the commodities STORAGE_ACCESS and STORAGE_PROVISIONED bought from a storage it saves
 *          their cluster id, VM id, producer (storage) id and used values.
 *      -For all these 11 commodities it saves the sum of used values and capacities per slice.
 */
public class SystemLoadSnapshot {

    private final Logger logger = LogManager.getLogger();

    // Tables to keep statistics for system load
    private Map<String, Double[]> slice2sumUsed = null;
    private Map<String, Double[]> slice2sumCapacities = null;
    private Multimap<String, Pair<CommoditySoldDTO, TopologyEntityDTO>> updatedSoldCommodities = null;
    private Multimap<String, Pair<CommodityBoughtDTO, TopologyEntityDTO>> updatedBoughtCommodities = null;

    public SystemLoadSnapshot() {}

    /**
     * The method saves information about the current system load commodities of
     * the system. This info is the commodities per slice and the sums of used values and
     * capacities per system load commodity per slice.
     *
     * @param allTopologyDTOs All the topology entities of the current snapshot.
     * @param groupServiceClient A client to request groups info from group component.
     */
    public void saveSnapshot(@Nonnull Collection<TopologyEntityDTO> allTopologyDTOs,
                             @Nonnull GroupServiceBlockingStub groupServiceClient,
                             @Nonnull SystemLoadHelper systemLoadHelper) {

        Map<String, Group> slice2groups = null;
        boolean savedData = false;

        // Initialize the tables to keep statistics for system load
        slice2sumUsed = Maps.newHashMap();
        slice2sumCapacities = Maps.newHashMap();
        updatedSoldCommodities = HashMultimap.create();
        updatedBoughtCommodities = HashMultimap.create();
        slice2groups = Maps.newHashMap();

        // Getting information for the groups of the system
        GetGroupsRequest groupsRequest = GetGroupsRequest.newBuilder()
            .addTypeFilter(Type.CLUSTER)
            .setClusterFilter(ClusterFilter.newBuilder()
                .setTypeFilter(ClusterInfo.Type.COMPUTE)
                .build())
            .build();
        Iterator<Group> groupIterator = groupServiceClient.getGroups(groupsRequest);

        // Saving host clusters info
        while (groupIterator.hasNext()) {
            Group curGroup = groupIterator.next();
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
                                updateOverallSoldCommodityUsage(slice, new Pair<>(commmSold, topologyEntity));
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
                                    updateOverallBoughtCommodityUsage(slice, new Pair<>(commBought, topologyEntity));
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
                                        if (cpuSums.containsKey(slice)) {
                                            cpuSums.put(slice, cpuSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            cpuSums.put(slice, commSold.getCapacity());
                                        }
                                        HostsSumCapacities.setCpu(cpuSums);
                                    }
                                    break;
                                case MEM:
                                    for (String slice : slices) {
                                        Map<String, Double> memSums = HostsSumCapacities.getMem();
                                        if (memSums.containsKey(slice)) {
                                            memSums.put(slice, memSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            memSums.put(slice, commSold.getCapacity());
                                        }
                                        HostsSumCapacities.setMem(memSums);
                                    }
                                    break;
                                case IO_THROUGHPUT:
                                    for (String slice : slices) {
                                        Map<String, Double> ioThroughputSums = HostsSumCapacities.getIoThroughput();
                                        if (ioThroughputSums.containsKey(slice)) {
                                            ioThroughputSums.put(slice, ioThroughputSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            ioThroughputSums.put(slice, commSold.getCapacity());
                                        }
                                        HostsSumCapacities.setIoThroughput(ioThroughputSums);
                                    }
                                    break;
                                case NET_THROUGHPUT:
                                    for (String slice : slices) {
                                        Map<String, Double> netThroughputSums = HostsSumCapacities.getNetThroughput();
                                        if (netThroughputSums.containsKey(slice)) {
                                            netThroughputSums.put(slice, netThroughputSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            netThroughputSums.put(slice, commSold.getCapacity());
                                        }
                                        HostsSumCapacities.setNetThroughput(netThroughputSums);
                                    }
                                    break;
                                case CPU_PROVISIONED:
                                    for (String slice : slices) {
                                        Map<String, Double> cpuProvisionedSums = HostsSumCapacities.getCpuProvisioned();
                                        if (cpuProvisionedSums.containsKey(slice)) {
                                            cpuProvisionedSums.put(slice, cpuProvisionedSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            cpuProvisionedSums.put(slice, commSold.getCapacity());
                                        }
                                        HostsSumCapacities.setCpuProvisioned(cpuProvisionedSums);
                                    }
                                    break;
                                case MEM_PROVISIONED:
                                    for (String slice : slices) {
                                        Map<String, Double> memProvisionedSums = HostsSumCapacities.getMemProvisioned();
                                        if (memProvisionedSums.containsKey(slice)) {
                                            memProvisionedSums.put(slice, memProvisionedSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            memProvisionedSums.put(slice, commSold.getCapacity());
                                        }
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
                                        if (storageAccessSums.containsKey(slice)) {
                                            storageAccessSums.put(slice, storageAccessSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            storageAccessSums.put(slice, commSold.getCapacity());
                                        }
                                        StoragesSumCapacities.setStorageAccess(storageAccessSums);
                                    }
                                    break;
                                case STORAGE_PROVISIONED:
                                    for (String slice : slices) {
                                        Map<String, Double> storageProvisionedSums = StoragesSumCapacities.getStorageProvisioned();
                                        if (storageProvisionedSums.containsKey(slice)) {
                                            storageProvisionedSums.put(slice, storageProvisionedSums.get(slice) + commSold.getCapacity());
                                        } else {
                                            storageProvisionedSums.put(slice, commSold.getCapacity());
                                        }
                                        StoragesSumCapacities.setStorageProvisioned(storageProvisionedSums);
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
    }

    /**
     * The method updates the sums of used values and capacities for each commodity sold
     * by a VM and adds this commodity to the commodities of the slice.
     *
     * @param slice The slice that will be updated.
     * @param commodity The commodity to read the used value and the capacity.
     */
    public void updateOverallSoldCommodityUsage(String slice, Pair<CommoditySoldDTO, TopologyEntityDTO> commodity) {

        SystemLoadCommodities loadComm = SystemLoadCommodities.toSystemLoadCommodity(commodity.first.getCommodityType().getType());

        // Update sums of used values for the system load commodities
        Double[] used = slice2sumUsed.get(slice);
        if (used == null) {
            used = new Double[SystemLoadCommodities.SIZE];
            slice2sumUsed.put(slice, used);
        }
        used[loadComm.idx] = (used[loadComm.idx] == null) ? commodity.first.getUsed()
                : used[loadComm.idx] + commodity.first.getUsed();

        // Update sums of capacities for the system load commodities
        Double[] capacities = slice2sumCapacities.get(slice);
        if (capacities == null) {
            capacities = new Double[SystemLoadCommodities.SIZE];
            slice2sumCapacities.put(slice, capacities);
        }
        capacities[loadComm.idx] = (capacities[loadComm.idx] == null) ? commodity.first.getCapacity()
                : capacities[loadComm.idx] + commodity.first.getCapacity();

        // Keep track of the commodities
        updatedSoldCommodities.put(slice, commodity);
    }

    /**
     * The method updates the sums of used values for each commodity bought
     * by a VM and adds this commodity to the commodities of the slice.
     *
     * @param slice The slice that will be updated.
     * @param commodity The commodity to read the used value and the capacity.
     */
    public void updateOverallBoughtCommodityUsage(String slice, Pair<CommodityBoughtDTO, TopologyEntityDTO> commodity) {

        SystemLoadCommodities loadComm = SystemLoadCommodities.toSystemLoadCommodity(commodity.first.getCommodityType().getType());

        // Update sums of used values for the system load commodities
        Double[] used = slice2sumUsed.get(slice);
        if (used == null) {
            used = new Double[SystemLoadCommodities.SIZE];
            slice2sumUsed.put(slice, used);
        }
        used[loadComm.idx] = (used[loadComm.idx] == null) ? commodity.first.getUsed()
                : used[loadComm.idx] + commodity.first.getUsed();

        // Update sums of capacities for the system load commodities
        Double[] capacities = slice2sumCapacities.get(slice);
        if (capacities == null) {
            capacities = new Double[SystemLoadCommodities.SIZE];
            slice2sumCapacities.put(slice, capacities);
        }
        capacities[loadComm.idx] = 0.0;

        // Keep track of the commodities
        updatedBoughtCommodities.put(slice, commodity);
    }

}