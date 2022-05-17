package com.vmturbo.market.runner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * This class has methods to create fake TopologyEntityDTOs. Fake entities are needed for Suspension throttling and cluster entities.
 * There are 2 types of fake entities created today by this class:
 * 1. Fake VMs for Suspension throttling - one VM per cluster/storage cluster
 * 2. Fake Cluster entities which sell over provisioned commodities that is used to make sure the cluster's capacity
 *    is not exceeded. Cluster is not a TopologyEntityDTO (its only a group today), so we create a fake cluster entity
 *    to represent it.
 */
public class FakeEntityCreator {

    private final Logger logger = LogManager.getLogger();

    private final GroupMemberRetriever groupMemberRetriever;

    private final Set<Long> fakeClusterOids = new HashSet<>();

    /**
     * Constructor.
     * @param groupMemberRetriever the group member retriever
     */
    public FakeEntityCreator(GroupMemberRetriever groupMemberRetriever) {
        this.groupMemberRetriever = groupMemberRetriever;
    }

    /**
     * <p>There are 2 types of fake entities created today by this method:
     *  1. Fake VMs for Suspension throttling - one VM per cluster/storage cluster
     *     This is to ensure each cluster/storage cluster will form a unique market regardless of
     *     segmentation constraint which may divide the cluster/storage cluster.
     *  2. Fake Cluster entities which sell over provisioned commodities that is used to make sure the cluster's capacity
     *     is not exceeded. Cluster is not a TopologyEntityDTO (its only a group today), so we create a fake cluster entity
     *     to represent it.
     * </p>
     * @param topologyDTOs a map of oid to the TopologyEntityDTO
     * @param enableThrottling is suspension throttling enabled
     * @param enableOP is OP feature enabled
     * @return a set of fake TopologyEntityDTOS with only cluster/storage cluster commodity in the
     * commodity bought list
     */
    protected Map<Long, TopologyEntityDTO> createFakeTopologyEntityDTOs(
            Map<Long, TopologyEntityDTO> topologyDTOs, boolean enableThrottling, boolean enableOP) {
        // create fake entities to help construct markets in which sellers of a compute
        // or a storage cluster serve as market sellers
        Set<TopologyEntityDTO> fakeEntityDTOs = new HashSet<>();
        try {
            Map<String, Set<TopologyEntityDTO>> clusterKeyToHost = new HashMap<>();
            Set<TopologyEntityDTO> pmEntityDTOs = getEntityDTOsInCluster(GroupType.COMPUTE_HOST_CLUSTER, topologyDTOs);
            Set<TopologyEntityDTO> dsEntityDTOs = getEntityDTOsInCluster(GroupType.STORAGE_CLUSTER, topologyDTOs);
            Set<String> dsClusterCommKeySet = new HashSet<>();
            pmEntityDTOs.forEach(dto -> {
                dto.getCommoditySoldListList().forEach(commSold -> {
                    if (commSold.getCommodityType().getType() == CommodityType.CLUSTER_VALUE) {
                        clusterKeyToHost.computeIfAbsent(commSold.getCommodityType().getKey(), val -> new HashSet<>()).add(dto);
                    }
                });
            });
            dsEntityDTOs.forEach(dto -> {
                dto.getCommoditySoldListList().forEach(commSold -> {
                    if (commSold.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE
                            && isRealStorageClusterCommodity(commSold)) {
                        dsClusterCommKeySet.add(commSold.getCommodityType().getKey());
                    }
                });
            });
            if (enableThrottling) {
                clusterKeyToHost.forEach((key, hosts) -> {
                    fakeEntityDTOs.add(createFakeDTOs(CommodityType.CLUSTER_VALUE, key));
                });
                dsClusterCommKeySet.forEach(key -> fakeEntityDTOs
                        .add(createFakeDTOs(CommodityType.STORAGE_CLUSTER_VALUE, key)));
            }
            if (enableOP) {
                createFakeClusters(topologyDTOs, clusterKeyToHost, fakeEntityDTOs);
            }
        } catch (StatusRuntimeException e) {
            logger.error("Failed to get cluster members from group component due to: {}."
                    + " Not creating fake entity DTOs for suspension throttling.", e.getMessage());
        }
        return fakeEntityDTOs.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getOid,
                        Function.identity()));
    }

    /**
     * Create fake clusters for each unique cluster key. The cluster entity should sell CPU_PROVISIONED and
     * MEM_PROVISIONED and the hosts in a cluster should buy CPU_PROVISIONED and MEM_PROVISIONED from that fake cluster entity.
     * For the provisioned commodity sold by cluster:
     * 1. capacity is the sum of the capacities of the corresponding commodity of all the hosts in that cluster.
     * 2. Their effective capacity percentage is the the percentage of the sum of the effective capacities of the hosts
     * to the sum of the capacities of the hosts.
     * 3. used is the sum of the used of the corresponding commodity of all the hosts in that cluster.
     * @param topologyDTOs the current topology
     * @param clusterKeyToHost map of cluster key to set of hosts in that cluster
     * @param fakeEntityDTOs the created cluster entities need to be added to this result set
     */
    private void createFakeClusters(Map<Long, TopologyEntityDTO> topologyDTOs,
                                    Map<String, Set<TopologyEntityDTO>> clusterKeyToHost,
                                    Set<TopologyEntityDTO> fakeEntityDTOs) {
        int numClustersCreated = 0;
        for (String key : clusterKeyToHost.keySet()) {
            Set<TopologyEntityDTO> hosts = clusterKeyToHost.get(key);
            double clusterCpuProvCapacity = 0;
            double clusterMemProvCapacity = 0;
            double clusterCpuProvEffectiveCapacity = 0;
            double clusterMemProvEffectiveCapacity = 0;
            double clusterCpuProvUsed = 0;
            double clusterMemProvUsed = 0;
            TopologyEntityDTO.Builder clusterBuilder = createFakeClusterDTOBuilder(key);
            final CommodityBoughtDTO memCommBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.MEM_PROVISIONED_VALUE).setKey(key).build())
                    .build();
            final CommodityBoughtDTO cpuCommBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.CPU_PROVISIONED_VALUE).setKey(key).build())
                    .build();
            for (TopologyEntityDTO host : hosts) {
                TopologyEntityDTO.Builder hostBuilder = host.toBuilder();
                hostBuilder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(memCommBought).addCommodityBought(cpuCommBought)
                        .setMovable(false).setProviderId(clusterBuilder.getOid()).setProviderEntityType(EntityType.CLUSTER_VALUE));
                for (CommoditySoldDTO.Builder commSoldBuilder : hostBuilder.getCommoditySoldListBuilderList()) {
                    if (commSoldBuilder.getCommodityType().getType() == CommodityType.CPU_PROVISIONED_VALUE) {
                        clusterCpuProvCapacity += commSoldBuilder.getCapacity();
                        clusterCpuProvUsed += commSoldBuilder.getUsed();
                        clusterCpuProvEffectiveCapacity += commSoldBuilder.getEffectiveCapacityPercentage() / 100 * commSoldBuilder.getCapacity();
                        commSoldBuilder.setIsResold(true);
                    } else if (commSoldBuilder.getCommodityType().getType() == CommodityType.MEM_PROVISIONED_VALUE) {
                        clusterMemProvCapacity += commSoldBuilder.getCapacity();
                        clusterMemProvUsed += commSoldBuilder.getUsed();
                        clusterMemProvEffectiveCapacity += commSoldBuilder.getEffectiveCapacityPercentage() / 100 * commSoldBuilder.getCapacity();
                        commSoldBuilder.setIsResold(true);
                    }
                }
                topologyDTOs.put(hostBuilder.getOid(), hostBuilder.build());
            }
            double cpuEffectiveCapacityPercentage = clusterCpuProvEffectiveCapacity / clusterCpuProvCapacity * 100;
            double memEffectiveCapacityPercentage = clusterMemProvEffectiveCapacity / clusterMemProvCapacity * 100;
            for (CommoditySoldDTO.Builder commSoldBuilder : clusterBuilder.getCommoditySoldListBuilderList()) {
                if (commSoldBuilder.getCommodityType().getType() == CommodityType.CPU_PROVISIONED_VALUE) {
                    setClusterCommSoldProperties(commSoldBuilder, clusterCpuProvCapacity, cpuEffectiveCapacityPercentage, clusterCpuProvUsed);
                } else if (commSoldBuilder.getCommodityType().getType() == CommodityType.MEM_PROVISIONED_VALUE) {
                    setClusterCommSoldProperties(commSoldBuilder, clusterMemProvCapacity, memEffectiveCapacityPercentage, clusterMemProvUsed);
                }
            }
            TopologyEntityDTO cluster = clusterBuilder.build();
            fakeEntityDTOs.add(cluster);
            numClustersCreated++;
        }
        logger.info("Created {} cluster entities", numClustersCreated);
    }

    private void setClusterCommSoldProperties(CommoditySoldDTO.Builder commSoldBuilder, double capacity, double effCapPercentage, double used) {
        commSoldBuilder.setCapacity(capacity);
        commSoldBuilder.setEffectiveCapacityPercentage(effCapPercentage);
        commSoldBuilder.setUsed(used);
    }

    /**
     * Modifies the hosts (PHYSICAL_MACHINE) in the topology and removes the cluster commodity bought grouping from the hosts.
     * @param topology the topology to be modified
     */
    public void removeClusterCommBoughtGroupingOfHosts(Map<Long, TopologyEntityDTO> topology) {
        topology.replaceAll((oid, entity) -> removeClusterCommBoughtGroupingOfHost(entity));
    }

    private TopologyEntityDTO removeClusterCommBoughtGroupingOfHost(TopologyEntityDTO entity) {
        if (entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
            TopologyEntityDTO.Builder hostBuilder = entity.toBuilder();
            OptionalInt indexOfClusterCommBoughtGrouping = IntStream.range(0, hostBuilder.getCommoditiesBoughtFromProvidersList().size())
                    .filter(i -> hostBuilder.getCommoditiesBoughtFromProviders(i).getProviderEntityType() == EntityType.CLUSTER_VALUE)
                    .findFirst();
            indexOfClusterCommBoughtGrouping.ifPresent(hostBuilder::removeCommoditiesBoughtFromProviders);
            return hostBuilder.build();
        }
        return entity;
    }

    /**
     * Create a Cluster Entity TopologyEntityDTO to sell OP commodities to PMs.
     *
     * @param key the commodity's key
     * @return a cluster TopologyEntityDTO.Builder to keep it mutable
     */
    private TopologyEntityDTO.Builder createFakeClusterDTOBuilder(String key) {
        CommoditySoldDTO memProvSold = CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.MEM_PROVISIONED_VALUE)
                        .setKey(key).build())
                .setIsResizeable(false)
                .build();
        CommoditySoldDTO cpuProvSold = CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.CPU_PROVISIONED_VALUE)
                        .setKey(key).build())
                .setIsResizeable(false)
                .build();
        long id = IdentityGenerator.next();
        fakeClusterOids.add(id);
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CLUSTER_VALUE)
                .setOid(id)
                .setDisplayName("FakeCluster-" + key)
                .setEntityState(EntityState.POWERED_ON)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(false))
                .addCommoditySoldList(memProvSold)
                .addCommoditySoldList(cpuProvSold);
    }

    protected Set<TopologyEntityDTO> getEntityDTOsInCluster(GroupType groupType, Map<Long, TopologyEntityDTO> topologyDTOs) {
        Set<TopologyEntityDTO> entityDTOs = new HashSet<>();
        groupMemberRetriever.getGroupsWithMembers(GroupDTO.GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupDTO.GroupFilter.newBuilder()
                        .setGroupType(groupType))
                .build())
                .forEach(groupAndMembers -> {
                    for (long memberId : groupAndMembers.members()) {
                        if (topologyDTOs.containsKey(memberId)) {
                            entityDTOs.add(topologyDTOs.get(memberId));
                        }
                    }
                });
        return entityDTOs;
    }

    /**
     * Create fake VM TopologyEntityDTOs to buy cluster/storage cluster commodity only.
     *
     * @param clusterValue cluster or storage cluster
     * @param key the commodity's key
     * @return a VM TopologyEntityDTO
     */
    private TopologyEntityDTO createFakeDTOs(int clusterValue, String key) {
        final CommodityBoughtDTO clusterCommBought = CommodityBoughtDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(clusterValue).setKey(key).build())
                .build();
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(IdentityGenerator.next())
                .setDisplayName("FakeVM-" + clusterValue + key)
                .setEntityState(EntityState.POWERED_ON)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(false).build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(clusterCommBought).build())
                .build();
    }

    /**
     * <p>Check if the commoditySoldDTO is a storage cluster commodity for real storage cluster.
     * Real storage cluster is a storage cluster that is physically exits in the data center.
     * </p>
     * @param comm commoditySoldDTO
     * @return true if it is for real cluster
     */
    private boolean isRealStorageClusterCommodity(CommoditySoldDTO comm) {
        return comm.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE
                && TopologyDTOUtil.isRealStorageClusterCommodityKey(comm.getCommodityType().getKey());
    }

    /**
     * Is the oid a fake cluster oid ?
     * @param oid the oid to check
     * @return true if it is a fake cluster oid
     */
    public boolean isFakeClusterOid(long oid) {
        return fakeClusterOids.contains(oid);
    }
}
