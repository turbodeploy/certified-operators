package com.vmturbo.market.runner;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Tests for FakeEntityCreator.
 */
public class FakeEntityCreatorTest {

    private static final String CLUSTER1_KEY = "key1";
    private static final String CLUSTER2_KEY = "key2";
    private static final double DELTA = 0.01;
    private FakeEntityCreator fakeEntityCreator;
    private Map<Long, TopologyEntityDTO> topologyDTOs;
    private Map<Long, TopologyEntityDTO> result;

    /**
     * Create 4 hosts - 2 from cluster 1 and 2 from cluster 2.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(1L);
        topologyDTOs = Maps.newHashMap();
        TopologyEntityDTO host1 = host(1L, CLUSTER1_KEY, 1000, 3000, 90, 90, 10, 20);
        topologyDTOs.put(host1.getOid(), host1);
        TopologyEntityDTO host2 = host(2L, CLUSTER1_KEY, 500, 700, 80, 80, 30, 40);
        topologyDTOs.put(host2.getOid(), host2);
        TopologyEntityDTO host3 = host(3L, CLUSTER2_KEY, 2000, 4000, 90, 80, 50, 60);
        topologyDTOs.put(host3.getOid(), host3);
        TopologyEntityDTO host4 = host(4L, CLUSTER2_KEY, 600, 800, 90, 80, 70, 80);
        topologyDTOs.put(host4.getOid(), host4);
        TopologyEntityDTO vm1 = vm(101L, 1L, CLUSTER1_KEY);
        topologyDTOs.put(vm1.getOid(), vm1);
        TopologyEntityDTO vm2 = vm(102L, 2L, CLUSTER1_KEY);
        topologyDTOs.put(vm2.getOid(), vm2);
        TopologyEntityDTO vm3 = vm(103L, 3L, CLUSTER2_KEY);
        topologyDTOs.put(vm3.getOid(), vm3);
        TopologyEntityDTO vm4 = vm(104L, 4L, CLUSTER2_KEY);
        topologyDTOs.put(vm4.getOid(), vm4);
        GroupAndMembers cluster1 = ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Arrays.asList(1L, 2L)).entities(Arrays.asList(1L, 2L)).build();
        GroupAndMembers cluster2 = ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Arrays.asList(3L, 4L)).entities(Arrays.asList(3L, 4L)).build();
        GroupMemberRetriever mockGroupMemberRetriever = mock(GroupMemberRetriever.class);
        GetGroupsRequest clusterRequest = GetGroupsRequest.newBuilder().setGroupFilter(GroupDTO.GroupFilter.newBuilder()
                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)).build();
        when(mockGroupMemberRetriever.getGroupsWithMembers(clusterRequest))
                .thenReturn(Arrays.asList(cluster1, cluster2));
        fakeEntityCreator = new FakeEntityCreator(mockGroupMemberRetriever);
        result = fakeEntityCreator.createFakeTopologyEntityDTOs(topologyDTOs, true, true);
    }

    /**
     * 1. Test that 2 cluster entities are created.
     * 2. The cluster entities should sell CPU_PROVISIONED and MEM_PROVISIONED.
     *  a. Their capacities should be the sum of the capacities of the hosts.
     *  b. Their effective capacity percentage should be the the percentage of the sum of the effective capacities of
     * the hosts to the sum of the capacities of the hosts.
     *  c. Their used values should be the sum of the used values of the hosts.
     */
    @Test
    public void testClustersCommSold() {
        Assert.assertEquals(2, result.size());
        int assertCount = 0;
        for (TopologyEntityDTO cluster : result.values()) {
            Assert.assertEquals(EntityType.CLUSTER_VALUE, cluster.getEntityType());
            Assert.assertTrue(fakeEntityCreator.isFakeComputeClusterOid(cluster.getOid()));
            for (CommoditySoldDTO cs : cluster.getCommoditySoldListList()) {
                if (cs.getCommodityType().getType() == CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE) {
                    if (cluster.getDisplayName().equals("FakeCluster-0key1")) {
                        Assert.assertEquals(1500, cs.getCapacity(), DELTA);
                        Assert.assertEquals(86.66, cs.getEffectiveCapacityPercentage(), DELTA);
                        Assert.assertEquals(40, cs.getUsed(), DELTA);
                        assertCount++;
                    }
                    if (cluster.getDisplayName().equals("FakeCluster-0key2")) {
                        Assert.assertEquals(2600, cs.getCapacity(), DELTA);
                        Assert.assertEquals(90, cs.getEffectiveCapacityPercentage(), DELTA);
                        Assert.assertEquals(120, cs.getUsed(), DELTA);
                        assertCount++;
                    }
                }
                if (cs.getCommodityType().getType() == CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE) {
                    if (cluster.getDisplayName().equals("FakeCluster-0key1")) {
                        Assert.assertEquals(3700, cs.getCapacity(), DELTA);
                        Assert.assertEquals(88.1, cs.getEffectiveCapacityPercentage(), DELTA);
                        Assert.assertEquals(60, cs.getUsed(), DELTA);
                        assertCount++;
                    }
                    if (cluster.getDisplayName().equals("FakeCluster-0key2")) {
                        Assert.assertEquals(4800, cs.getCapacity(), DELTA);
                        Assert.assertEquals(80, cs.getEffectiveCapacityPercentage(), DELTA);
                        Assert.assertEquals(140, cs.getUsed(), DELTA);
                        assertCount++;
                    }
                }
                if (cs.getCommodityType().getType() == CommodityDTO.CommodityType.ACCESS_VALUE) {
                    if (cluster.getDisplayName().equals("FakeCluster-0key1")) {
                        Assert.assertEquals(TopologyConversionConstants.ACCESS_COMMODITY_CAPACITY, cs.getCapacity(), DELTA);
                        assertCount++;
                    }
                    if (cluster.getDisplayName().equals("FakeCluster-0key2")) {
                        Assert.assertEquals(TopologyConversionConstants.ACCESS_COMMODITY_CAPACITY, cs.getCapacity(), DELTA);
                        assertCount++;
                    }
                }
            }
        }
        Assert.assertEquals(6, assertCount);
    }

    /**
     * The Clusters should have shopping lists supplied by the hosts.
     * The Clusters should buy CPU_PROVISIONED and MEM_PROVISIONED from the cluster.
     */
    @Test
    public void testClustersCommBought() {
        Set<Long> expectedHostSuppliers;
        Set<CommodityType> expectedCommsBought;
        final CommodityType memProv = CommodityType.newBuilder()
                .setType(CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE).build();
        final CommodityType cpuProv = CommodityType.newBuilder()
                .setType(CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE).build();
        for (TopologyEntityDTO cluster : result.values()) {
            Assert.assertEquals(2, cluster.getCommoditiesBoughtFromProvidersCount());
            if (cluster.getCommoditySoldListList().stream().anyMatch(cs -> cs.getCommodityType().getKey().equals(StringConstants.FAKE_CLUSTER_COMMODITY_PREFIX + '|' + CLUSTER1_KEY))) {
                expectedHostSuppliers = ImmutableSet.of(1L, 2L);
                final CommodityType clusterComm = CommodityType.newBuilder()
                        .setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE).setKey(CLUSTER1_KEY).build();
                expectedCommsBought = ImmutableSet.of(memProv, cpuProv, clusterComm);
            } else {
                expectedHostSuppliers = ImmutableSet.of(3L, 4L);
                final CommodityType clusterComm = CommodityType.newBuilder()
                        .setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE).setKey(CLUSTER2_KEY).build();
                expectedCommsBought = ImmutableSet.of(memProv, cpuProv, clusterComm);
            }
            Set<Long> actualHostSuppliers = cluster.getCommoditiesBoughtFromProvidersList().stream()
                    .map(g -> g.getProviderId()).collect(Collectors.toSet());
            Assert.assertEquals(expectedHostSuppliers, actualHostSuppliers);
            for (CommoditiesBoughtFromProvider commBoughtGrouping : cluster.getCommoditiesBoughtFromProvidersList()) {
                Set<CommodityType> actualCommBought = commBoughtGrouping.getCommodityBoughtList().stream()
                        .map(c -> c.getCommodityType()).collect(Collectors.toSet());
                Assert.assertEquals(expectedCommsBought, actualCommBought);
                Assert.assertFalse(commBoughtGrouping.getMovable());
                Assert.assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, commBoughtGrouping.getProviderEntityType());
                Assert.assertEquals(0, commBoughtGrouping.getCommodityBoughtList().stream().mapToDouble(c -> c.getUsed()).sum(), DELTA);
            }
        }
    }

    /**
     * Test that VMs will buy from clusters after calling createFakeTopologyEntityDTOs.
     */
    @Test
    public void testLinkVmsToCluster() {
        Map<String, TopologyEntityDTO> clusterByKey = Maps.newHashMap();
        for (TopologyEntityDTO cluster : result.values()) {
            String clusterKey = cluster.getCommoditySoldListList().stream().filter(commSold ->
                    commSold.getCommodityType().getType() == CommodityDTO.CommodityType.CLUSTER_VALUE)
                    .map(c -> c.getCommodityType().getKey()).findFirst().get();
            clusterByKey.put(clusterKey, cluster);
        }
        Set<TopologyEntityDTO> vms = topologyDTOs.values().stream()
                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE).collect(Collectors.toSet());
        Assert.assertEquals(4, vms.size());
        for (TopologyEntityDTO vm : vms) {
            Assert.assertEquals(2, vm.getCommoditiesBoughtFromProvidersCount());
            Optional<CommoditiesBoughtFromProvider> clusterSl =  vm.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(g -> g.getProviderEntityType() == EntityType.CLUSTER_VALUE)
                    .findFirst();
            Assert.assertTrue(clusterSl.isPresent());
            String key;
            if (vm.getOid() == 101L || vm.getOid() == 102L) {
                key = CLUSTER1_KEY;
            } else {
                key = CLUSTER2_KEY;
            }
            long clusterOid = clusterByKey.get(StringConstants.FAKE_CLUSTER_COMMODITY_PREFIX + '|' + key).getOid();
            CommodityType cpuProvType = CommodityType.newBuilder()
                    .setType(CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE).build();
            CommodityType memProvType = CommodityType.newBuilder()
                    .setType(CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE).build();
            CommodityType clusterType = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
                    .setKey(StringConstants.FAKE_CLUSTER_COMMODITY_PREFIX + '|' + key).build();
            CommodityType accessType = CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.ACCESS_VALUE).setKey(StringConstants.FAKE_CLUSTER_ACCESS_COMMODITY_KEY).build();

            Assert.assertEquals(ImmutableSet.of(cpuProvType, memProvType, clusterType, accessType),
                    clusterSl.get().getCommodityBoughtList().stream().map(c -> c.getCommodityType()).collect(Collectors.toSet()));
            Assert.assertEquals(clusterOid, clusterSl.get().getProviderId());
        }
    }

    /**
     * When removeClusterCommBoughtGroupingOfHosts is called for the topology, the links between Vms and clusters
     * should be removed.
     */
    @Test
    public void testRemoveClusterCommBoughtGroupingOfVms() {
        fakeEntityCreator.removeClusterCommBoughtGroupingOfVms(topologyDTOs);
        Set<TopologyEntityDTO> vms = topologyDTOs.values().stream()
                .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE).collect(Collectors.toSet());
        Assert.assertEquals(4, vms.size());
        for (TopologyEntityDTO vm : vms) {
            Assert.assertEquals(1, vm.getCommoditiesBoughtFromProvidersCount());
            Assert.assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, vm.getCommoditiesBoughtFromProviders(0).getProviderEntityType());
        }
    }

    private CommoditySoldDTO.Builder commoditySold(CommodityType.Builder commType, double capacity, double effectiveCapPerc, double used) {
        return CommoditySoldDTO.newBuilder().setCommodityType(commType)
                .setCapacity(capacity)
                .setEffectiveCapacityPercentage(effectiveCapPerc)
                .setUsed(used);
    }

    private CommodityType.Builder commodityType(int type, Optional<String> key) {
        CommodityType.Builder commType = CommodityType.newBuilder().setType(type);
        if (key.isPresent()) {
            commType.setKey(key.get());
        }
        return commType;
    }

    private TopologyEntityDTO host(Long oid, String clusterKey, double cpuProvCapacity, double memProvCapacity,
                                   double cpuEffectiveCapPerc, double memEffectiveCapPerc,
                                   double cpuUsed, double memUsed) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setOid(oid)
                .addCommoditySoldList(commoditySold(commodityType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE,
                        Optional.of(clusterKey)), 0, 100, 0))
                .addCommoditySoldList(commoditySold(commodityType(CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE,
                        Optional.empty()), cpuProvCapacity, cpuEffectiveCapPerc, cpuUsed))
                .addCommoditySoldList(commoditySold(commodityType(CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE,
                        Optional.empty()), memProvCapacity, memEffectiveCapPerc, memUsed))
                .build();
    }

    private TopologyEntityDTO vm(Long oid, long hostProviderOid, String clusterKey) {
        final CommodityType clusterCommType = CommodityType.newBuilder()
                .setType(CommonDTO.CommodityDTO.CommodityType.CLUSTER_VALUE).setKey(clusterKey).build();
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(oid)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(hostProviderOid)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE).build()))
                        .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE).build()))
                        .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(clusterCommType))
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build())
                .build();
    }
}