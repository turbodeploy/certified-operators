package com.vmturbo.market.runner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * There is 1 type of fake entity created today by this class:
 * Fake cluster entities that are used for 2 purposes:
 * 1. For Suspension throttling - one VM per cluster/storage cluster
 *    This is to ensure each cluster/storage cluster will form a unique market regardless of
 *    segmentation constraint which may divide the cluster/storage cluster.
 * 2. These fake Cluster entities sell over provisioned commodities that is used to make sure the cluster's capacity
 *    is not exceeded. VMs buy OP commodities from this cluster, and the cluster buys from all the hosts that it has in it.
 *    Cluster is not a TopologyEntityDTO (its only a group today), so we create a fake cluster entity
 *    to represent it.
 */
public class FakeEntityCreator {

    private final Logger logger = LogManager.getLogger();

    private final GroupMemberRetriever groupMemberRetriever;

    private final Set<Long> fakeComputeClusterOids = new HashSet<>();

    private final HashMap<Long, Long> hostIdToClusterId = new HashMap<>();

    /**
     * Constructor.
     * @param groupMemberRetriever the group member retriever
     */
    public FakeEntityCreator(GroupMemberRetriever groupMemberRetriever) {
        this.groupMemberRetriever = groupMemberRetriever;
    }

    /**
     * <p>There is 1 type of fake entity created today by this method:
     * Fake cluster entities that are used for 2 purposes:
     * 1. For Suspension throttling - one VM per cluster/storage cluster
     *    This is to ensure each cluster/storage cluster will form a unique market regardless of
     *    segmentation constraint which may divide the cluster/storage cluster.
     * 2. These fake Cluster entities sell over provisioned commodities that is used to make sure the cluster's capacity
     *    is not exceeded. VMs buy OP commodities from this cluster, and the cluster buys from all the hosts that it has in it.
     *    Cluster is not a TopologyEntityDTO (its only a group today), so we create a fake cluster entity
     *    to represent it.
     * </p>
     * @param topologyDTOs a map of oid to the TopologyEntityDTO
     * @param enableThrottling is suspension throttling enabled
     * @param enableOP is OP feature enabled
     * @return a set of fake TopologyEntityDTOS with only cluster/storage cluster commodity in the
     * commodity bought list
     */
    Map<Long, TopologyEntityDTO> createFakeTopologyEntityDTOs(
            final Map<Long, TopologyEntityDTO> topologyDTOs, boolean enableThrottling, boolean enableOP) {
        // create fake entities to help construct markets in which sellers of a compute
        // or a storage cluster serve as market sellers
        Set<TopologyEntityDTO> fakeEntityDTOs = new HashSet<>();
        try {
            Map<String, Set<TopologyEntityDTO>> clusterKeyToHost = new HashMap<>();
            Map<String, Set<TopologyEntityDTO>> clusterKeyToVms = new HashMap<>();
            Set<TopologyEntityDTO> unrestrictedVMs = new HashSet<>();
            Set<TopologyEntityDTO> pmEntityDTOs = getEntityDTOsInCluster(GroupType.COMPUTE_HOST_CLUSTER, topologyDTOs);
            Set<TopologyEntityDTO> dsEntityDTOs = getEntityDTOsInCluster(GroupType.STORAGE_CLUSTER, topologyDTOs);
            Set<String> dsClusterCommKeySet = new HashSet<>();
            for (TopologyEntityDTO pmEntityDTO : pmEntityDTOs) {
                pmEntityDTO.getCommoditySoldListList().forEach(commSold -> {
                    if (commSold.getCommodityType().getType()
                            == CommodityType.CLUSTER_VALUE
                            && commSold.getCommodityType().hasKey()) {
                        clusterKeyToHost.computeIfAbsent(commSold.getCommodityType().getKey(), val -> new HashSet<>()).add(pmEntityDTO);
                    }
                });
            }
            for (TopologyEntityDTO ds : dsEntityDTOs) {
                ds.getCommoditySoldListList().forEach(commSold -> {
                    if (commSold.getCommodityType().getType() == CommodityType.STORAGE_CLUSTER_VALUE
                            && isRealStorageClusterCommodity(commSold)) {
                        dsClusterCommKeySet.add(commSold.getCommodityType().getKey());
                    }
                });
            }
            for (TopologyEntityDTO entity : topologyDTOs.values()) {
                if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    Optional<CommoditiesBoughtFromProvider> computeSl = entity.getCommoditiesBoughtFromProvidersList().stream()
                            .filter(grouping -> grouping.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE).findFirst();
                    if (computeSl.isPresent()) {
                        Optional<String> clusterKey = computeSl.get().getCommodityBoughtList().stream()
                                .filter(c -> c.getCommodityType().getType()
                                        == CommodityType.CLUSTER_VALUE
                                        && c.getCommodityType().hasKey())
                                .map(c -> c.getCommodityType().getKey())
                                .findFirst();
                        if (clusterKey.isPresent()) {
                            clusterKeyToVms.computeIfAbsent(clusterKey.get(), k -> new HashSet<>()).add(entity);
                        } else {
                            unrestrictedVMs.add(entity);
                        }
                    }
                }
            }
            if (enableThrottling || enableOP) {
                clusterKeyToHost.forEach((key, hosts) -> {
                    TopologyEntityDTO.Builder clusterBuilder = createClusterDTOs(CommodityType.CLUSTER_VALUE, key);
                    if (enableOP) {
                        setAnalysisFlagsOnCluster(clusterBuilder);
                        fakeComputeClusterOids.add(clusterBuilder.getOid());
                        linkClusterToHosts(clusterBuilder, hosts, key);
                        addClusterCommsSold(clusterBuilder, hosts, key);
                        linkVmsToClusters(key, clusterKeyToVms, clusterBuilder, topologyDTOs);
                    }
                    fakeEntityDTOs.add(clusterBuilder.build());
                });
                if (enableOP) {
                    addCommBoughtToUnplacedVms(unrestrictedVMs, topologyDTOs);
                }
                logger.info("Created {} compute cluster entities buying from hosts", fakeComputeClusterOids.size());
                dsClusterCommKeySet.forEach(key -> fakeEntityDTOs
                        .add(createClusterDTOs(CommodityType.STORAGE_CLUSTER_VALUE, key).build()));
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
     * Setting controllable to true on the cluster is needed because the VM's cluster SL needs to
     * be movable true and this depends on the controllable flag of the supplier.
     * And because we set the controllable to true, we set all the other flags (that don't have a
     * default in the protobuf as false) as false.
     * We want fake clusters to have isAvailableAsProvider = true (default is true) because
     * they will be able to accept new customers, in ignore constraints plans.
     * @param clusterBuilder cluster
     */
    private void setAnalysisFlagsOnCluster(TopologyEntityDTO.Builder clusterBuilder) {
        clusterBuilder.getAnalysisSettingsBuilder()
                .setControllable(true)
                .setCloneable(false)
                .setSuspendable(false)
                .setDeletable(false)
                .setIsEligibleForScale(false)
                .setReconfigurable(false);
    }

    /**
     * Creates one shopping list per host that the cluster has. This shopping list has CPU_PROVISIONED and
     * MEM_PROVISIONED and is supplied by a host.
     * @param clusterBuilder the cluster
     * @param hosts hosts that are part of this cluster
     * @param clusterKey cluster key
     */
    private void linkClusterToHosts(final TopologyEntityDTO.Builder clusterBuilder, final Set<TopologyEntityDTO> hosts, String clusterKey) {
        clusterBuilder.clearCommoditiesBoughtFromProviders();
        for (TopologyEntityDTO host : hosts) {
            final CommodityBoughtDTO memCommBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.MEM_PROVISIONED_VALUE).build())
                    .build();
            final CommodityBoughtDTO cpuCommBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.CPU_PROVISIONED_VALUE).build())
                    .build();
            final CommodityBoughtDTO clusterCommBought = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                            .setType(CommodityType.CLUSTER_VALUE)
                            .setKey(clusterKey).build())
                    .build();
            clusterBuilder.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addCommodityBought(cpuCommBought)
                    .addCommodityBought(memCommBought)
                    .addCommodityBought(clusterCommBought)
                    .setProviderId(host.getOid())
                    .setMovable(false)
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE));
            hostIdToClusterId.put(host.getOid(), clusterBuilder.getOid());
        }
    }

    /**
     * This method sets properties like capacity, effective capacity percentage and used of commdodities sold by the cluster.
     * For the provisioned commodity sold by cluster:
     * a. capacity is the sum of the capacities of the corresponding commodity of all the hosts in that cluster.
     * b. Their effective capacity percentage is the the percentage of the sum of the effective capacities of the hosts
     *    to the sum of the capacities of the hosts.
     * c. used is the sum of the used of the corresponding commodity of all the hosts in that cluster.
     * @param clusterBuilder the cluster
     * @param hosts the hosts that are part of the cluster
     * @param clusterKey the key for the cluster
     */
    private void addClusterCommsSold(final TopologyEntityDTO.Builder clusterBuilder,
                                     final Set<TopologyEntityDTO> hosts,
                                     final String clusterKey) {
        double clusterCpuProvCapacity = 0;
        double clusterMemProvCapacity = 0;
        double clusterCpuProvEffectiveCapacity = 0;
        double clusterMemProvEffectiveCapacity = 0;
        double clusterCpuProvUsed = 0;
        double clusterMemProvUsed = 0;
        for (TopologyEntityDTO host : hosts) {
            boolean cpuProvDone = false;
            boolean memProvDone = false;
            for (CommoditySoldDTO commSold : host.getCommoditySoldListList()) {
                if (cpuProvDone && memProvDone) {
                    break;
                }
                if (commSold.getCommodityType().getType() == CommodityType.CPU_PROVISIONED_VALUE) {
                    clusterCpuProvCapacity += commSold.getCapacity();
                    clusterCpuProvUsed += commSold.getUsed();
                    clusterCpuProvEffectiveCapacity += commSold.getEffectiveCapacityPercentage() / 100 * commSold.getCapacity();
                    cpuProvDone = true;
                } else if (commSold.getCommodityType().getType() == CommodityType.MEM_PROVISIONED_VALUE) {
                    clusterMemProvCapacity += commSold.getCapacity();
                    clusterMemProvUsed += commSold.getUsed();
                    clusterMemProvEffectiveCapacity += commSold.getEffectiveCapacityPercentage() / 100 * commSold.getCapacity();
                    memProvDone = true;
                }
            }
        }
        double cpuEffectiveCapacityPercentage = clusterCpuProvEffectiveCapacity / clusterCpuProvCapacity * 100;
        double memEffectiveCapacityPercentage = clusterMemProvEffectiveCapacity / clusterMemProvCapacity * 100;

        TopologyDTO.CommodityType.Builder cpuProvType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.CPU_PROVISIONED_VALUE);
        CommoditySoldDTO.Builder cpuProvSold = CommoditySoldDTO.newBuilder()
                .setCommodityType(cpuProvType)
                .setCapacity(clusterCpuProvCapacity)
                .setEffectiveCapacityPercentage(cpuEffectiveCapacityPercentage)
                .setUsed(clusterCpuProvUsed);
        TopologyDTO.CommodityType.Builder memProvType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.MEM_PROVISIONED_VALUE);
        CommoditySoldDTO.Builder memProvSold = CommoditySoldDTO.newBuilder()
                .setCommodityType(memProvType)
                .setCapacity(clusterMemProvCapacity)
                .setEffectiveCapacityPercentage(memEffectiveCapacityPercentage)
                .setUsed(clusterMemProvUsed);
        TopologyDTO.CommodityType.Builder clusterType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.CLUSTER_VALUE).setKey(StringConstants.FAKE_CLUSTER_COMMODITY_PREFIX +  '|' + clusterKey);
        CommoditySoldDTO.Builder clusterCommSold = CommoditySoldDTO.newBuilder()
                .setCommodityType(clusterType)
                .setCapacity(TopologyConversionConstants.ACCESS_COMMODITY_CAPACITY);
        TopologyDTO.CommodityType.Builder accessCommType = TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.ACCESS_VALUE).setKey(StringConstants.FAKE_CLUSTER_ACCESS_COMMODITY_KEY);
        CommoditySoldDTO.Builder accessCommSold = CommoditySoldDTO.newBuilder()
                .setCommodityType(accessCommType)
                .setCapacity(TopologyConversionConstants.ACCESS_COMMODITY_CAPACITY);

        clusterBuilder.addCommoditySoldList(cpuProvSold).addCommoditySoldList(memProvSold)
                .addCommoditySoldList(clusterCommSold).addCommoditySoldList(accessCommSold);
    }

    /**
     * Creates a shopping list bought by the VMs and supplied by the cluster. The shopping list will
     * be unplaced.
     * @param unrestrictedVms set of unrestricted vms
     * @param topologyDTOs the topology
     */
    private void addCommBoughtToUnplacedVms(Set<TopologyEntityDTO> unrestrictedVms,
            final Map<Long, TopologyEntityDTO> topologyDTOs) {
        for (TopologyEntityDTO vm : unrestrictedVms) {
            TopologyEntityDTO.Builder vmBuilder = addClusterCommBoughtProviderforVM(vm, Optional.empty());
            topologyDTOs.put(vmBuilder.getOid(), vmBuilder.build());
        }
    }

    /**
     * Creates a shopping list bought by the VMs and supplied by the cluster of the host this VM is on.
     * @param key the key for the cluster
     * @param clusterKeyToVms map of cluster key to the VMs in that cluster
     * @param cluster the cluster object to link the VMs to
     * @param topologyDTOs the topology
     */
    private void linkVmsToClusters(final String key,
                                   final Map<String, Set<TopologyEntityDTO>> clusterKeyToVms,
                                   final TopologyEntityDTO.Builder cluster,
                                   final Map<Long, TopologyEntityDTO> topologyDTOs) {
        for (TopologyEntityDTO vm : clusterKeyToVms.getOrDefault(key, new HashSet<>())) {
            TopologyEntityDTO.Builder vmBuilder = addClusterCommBoughtProviderforVM(vm, Optional.of(cluster));
            topologyDTOs.put(vmBuilder.getOid(), vmBuilder.build());
        }
    }

    /**
     * The VM will buy the comm bought provider which is derived from the comm bought from host.
     * The provisioned commodities are copied.
     * The cluster commodity is copied directly. we append a prefix to the key.
     * The access commodity is added to restrict the sl to clusters
     * The bought values in both shopping list is the same.
     *
     * @param vm the vm which is buying.
     * @param cluster the cluster which is selling
     * @return vm builder with the ne comm bought provider added.
     */
    private TopologyEntityDTO.Builder addClusterCommBoughtProviderforVM(TopologyEntityDTO vm,
            Optional<TopologyEntityDTO.Builder> cluster) {
        /*
         The cluster sells 4 commodities and the vm buys a subset of these.
         The  provisioned commodities ar4ee exactly same as the ones sold by host.
         The Cluster commodity is appeneded with a prefix. The prefix is added
         so that the cluster does not buy from itself.. The cluster is buying from host the
         cluster commodity and provisioned commodities..If we dont add the prefix then cluster can
         buy from itself.
         We also add a access commodity. the reservation related vms are not buying the cluster
         commodity. So to avaoid the vm's cluster shopping list to be supplied by host we add this
         access commodity.
         */
        TopologyEntityDTO.Builder vmBuilder = vm.toBuilder();
        Optional<CommoditiesBoughtFromProvider> computeSl =
                vm.getCommoditiesBoughtFromProvidersList().stream().filter(
                        grouping -> grouping.getProviderEntityType()
                                == EntityType.PHYSICAL_MACHINE_VALUE).findFirst();
        if (computeSl.isPresent()) {
            Optional<CommodityBoughtDTO> memProvCommBoughtFromHost =
                    computeSl.get().getCommodityBoughtList().stream().filter(
                            c -> c.getCommodityType().getType()
                                    == CommodityType.MEM_PROVISIONED_VALUE).findFirst();
            Optional<CommodityBoughtDTO> cpuProvCommBoughtFromHost =
                    computeSl.get().getCommodityBoughtList().stream().filter(
                            c -> c.getCommodityType().getType()
                                    == CommodityType.CPU_PROVISIONED_VALUE).findFirst();
            CommoditiesBoughtFromProvider.Builder commBoughtFromProvider =
                    CommoditiesBoughtFromProvider.newBuilder();
            final CommodityBoughtDTO accessCommBought =
                    CommodityBoughtDTO.newBuilder().setCommodityType(
                            TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.ACCESS_VALUE)
                                    .setKey(StringConstants.FAKE_CLUSTER_ACCESS_COMMODITY_KEY)
                                    .build())
                            .setUsed(1.0d)
                            .build();
            if (memProvCommBoughtFromHost.isPresent() && cpuProvCommBoughtFromHost.isPresent()) {
                commBoughtFromProvider.addCommodityBought(memProvCommBoughtFromHost.get())
                        .addCommodityBought(cpuProvCommBoughtFromHost.get())
                        .addCommodityBought(accessCommBought)
                        .setProviderEntityType(EntityType.CLUSTER_VALUE);
            } else {
                return vmBuilder;
            }
            if (cluster.isPresent()) {
                Optional<CommodityBoughtDTO> clusterCommBoughtFromHost =
                        computeSl.get()
                                .getCommodityBoughtList()
                                .stream()
                                .filter(c -> c.getCommodityType().getType()
                                        == CommodityType.CLUSTER_VALUE && c.getCommodityType()
                                        .hasKey())
                                .findFirst();
                if (clusterCommBoughtFromHost.isPresent()) {
                    String clusterKey = clusterCommBoughtFromHost.get().getCommodityType().getKey();
                    final CommodityBoughtDTO clusterCommBought = clusterCommBoughtFromHost.get().toBuilder()
                            .setCommodityType(
                                    TopologyDTO.CommodityType.newBuilder()
                                            .setType(CommodityType.CLUSTER_VALUE)
                                            .setKey(StringConstants.FAKE_CLUSTER_COMMODITY_PREFIX + '|' + clusterKey))
                                            .build();
                    commBoughtFromProvider.addCommodityBought(clusterCommBought);
                }
                commBoughtFromProvider.setProviderId(cluster.get().getOid());
            }
            vmBuilder.addCommoditiesBoughtFromProviders(commBoughtFromProvider);
        }
        return vmBuilder;
    }

    /**
     * Modifies the VMs (VIRTUAL_MACHINE) in the topology and removes the cluster commodity bought grouping from the VMs.
     * @param topology the topology to be modified
     */
    public void removeClusterCommBoughtGroupingOfVms(Map<Long, TopologyEntityDTO> topology) {
        topology.replaceAll((oid, entity) -> removeClusterCommBoughtGroupingOfVm(entity));
    }

    private TopologyEntityDTO removeClusterCommBoughtGroupingOfVm(TopologyEntityDTO entity) {
        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
            TopologyEntityDTO.Builder vmBuilder = entity.toBuilder();
            OptionalInt indexOfClusterCommBoughtGrouping = IntStream.range(0, vmBuilder.getCommoditiesBoughtFromProvidersList().size())
                    .filter(i -> vmBuilder.getCommoditiesBoughtFromProviders(i).getProviderEntityType() == EntityType.CLUSTER_VALUE)
                    .findFirst();
            indexOfClusterCommBoughtGrouping.ifPresent(vmBuilder::removeCommoditiesBoughtFromProviders);
            return vmBuilder.build();
        }
        return entity;
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
    private TopologyEntityDTO.Builder createClusterDTOs(int clusterValue, String key) {
        final CommodityBoughtDTO clusterCommBought = CommodityBoughtDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(clusterValue).setKey(key).build())
                .build();
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CLUSTER_VALUE)
                .setOid(IdentityGenerator.next())
                .setDisplayName("FakeCluster-" + clusterValue + key)
                .setEntityState(EntityState.POWERED_ON)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(false).build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(clusterCommBought).build());
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
    public boolean isFakeComputeClusterOid(long oid) {
        return fakeComputeClusterOids.contains(oid);
    }

    public Map<Long, Long> getHostIdToClusterId() {
        return Collections.unmodifiableMap(hostIdToClusterId);
    }
}
