package com.vmturbo.market.runner;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Tests for FakeEntityCreator.
 */
public class FakeEntityCreatorTest {

    private static final String CLUSTER1_KEY = "key1";
    private static final String CLUSTER2_KEY = "key2";
    private FakeEntityCreator fakeEntityCreator;
    private Map<Long, TopologyEntityDTO> topologyDTOs;

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
        GroupAndMembers cluster1 = ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Arrays.asList(1L, 2L)).entities(Arrays.asList(3L, 4L)).build();
        GroupAndMembers cluster2 = ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Arrays.asList(3L, 4L)).entities(Arrays.asList(3L, 4L)).build();
        GroupMemberRetriever mockGroupMemberRetriever = mock(GroupMemberRetriever.class);
        GetGroupsRequest clusterRequest = GetGroupsRequest.newBuilder().setGroupFilter(GroupDTO.GroupFilter.newBuilder()
                .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)).build();
        when(mockGroupMemberRetriever.getGroupsWithMembers(clusterRequest))
                .thenReturn(Arrays.asList(cluster1, cluster2));
        fakeEntityCreator = new FakeEntityCreator(mockGroupMemberRetriever);
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
    public void testCreateClusters() {
        Map<Long, TopologyEntityDTO> result = fakeEntityCreator.createFakeTopologyEntityDTOs(topologyDTOs, false, true);
        Assert.assertEquals(2, result.size());
        int assertCount = 0;
        for (TopologyEntityDTO cluster : result.values()) {
            Assert.assertEquals(EntityType.CLUSTER_VALUE, cluster.getEntityType());
            fakeEntityCreator.isFakeClusterOid(cluster.getOid());
            for (CommoditySoldDTO cs : cluster.getCommoditySoldListList()) {
                if (cs.getCommodityType().getType() == CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE) {
                    if (CLUSTER1_KEY.equals(cs.getCommodityType().getKey())) {
                        Assert.assertEquals(1500, cs.getCapacity(), 0.1);
                        Assert.assertEquals(86.66, cs.getEffectiveCapacityPercentage(), 0.1);
                        Assert.assertEquals(40, cs.getUsed(), 0.1);
                        assertCount++;
                    }
                    if (CLUSTER2_KEY.equals(cs.getCommodityType().getKey())) {
                        Assert.assertEquals(2600, cs.getCapacity(), 0.1);
                        Assert.assertEquals(90, cs.getEffectiveCapacityPercentage(), 0.1);
                        Assert.assertEquals(120, cs.getUsed(), 0.1);
                        assertCount++;
                    }
                }
                if (cs.getCommodityType().getType() == CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE) {
                    if (CLUSTER1_KEY.equals(cs.getCommodityType().getKey())) {
                        Assert.assertEquals(3700, cs.getCapacity(), 0.1);
                        Assert.assertEquals(88.1, cs.getEffectiveCapacityPercentage(), 0.1);
                        Assert.assertEquals(60, cs.getUsed(), 0.1);
                        assertCount++;
                    }
                    if (CLUSTER2_KEY.equals(cs.getCommodityType().getKey())) {
                        Assert.assertEquals(4800, cs.getCapacity(), 0.1);
                        Assert.assertEquals(80, cs.getEffectiveCapacityPercentage(), 0.1);
                        Assert.assertEquals(140, cs.getUsed(), 0.1);
                        assertCount++;
                    }
                }
            }
        }
        Assert.assertEquals(4, assertCount);
    }

    /**
     * The hosts should now have shopping lists supplied by the cluster.
     * The hosts should buy CPU_PROVISIONED and MEM_PROVISIONED from the cluster.
     * The CPU_PROVISIONED and MEM_PROVISIONED sold by the host should be marked as Resold.
     */
    @Test
    public void testHostsModification() {
        Map<Long, TopologyEntityDTO> result = fakeEntityCreator.createFakeTopologyEntityDTOs(topologyDTOs, false, true);
        int assertCount = 0;
        for (TopologyEntityDTO host : topologyDTOs.values()) {
            CommoditiesBoughtFromProvider hostSl =  host.getCommoditiesBoughtFromProviders(0);
            Assert.assertFalse(hostSl.getMovable());
            Assert.assertEquals(hostSl.getProviderEntityType(), CommonDTO.EntityDTO.EntityType.CLUSTER_VALUE);
            TopologyEntityDTO cluster = result.get(hostSl.getProviderId());
            TopologyDTO.CommodityBoughtDTO cpuProv = hostSl.getCommodityBoughtList().stream().filter(comm ->
                    CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE == comm.getCommodityType().getType()).findFirst().get();
            TopologyDTO.CommodityBoughtDTO memProv = hostSl.getCommodityBoughtList().stream().filter(comm ->
                    CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE == comm.getCommodityType().getType()).findFirst().get();
            if (host.getOid() == 1L || host.getOid() == 2L) {
                Assert.assertEquals(CLUSTER1_KEY, cpuProv.getCommodityType().getKey());
                Assert.assertEquals(CLUSTER1_KEY, memProv.getCommodityType().getKey());
                Assert.assertTrue(cluster.getCommoditySoldListList().stream().allMatch(cs -> CLUSTER1_KEY.equals(cs.getCommodityType().getKey())));
                assertCount++;
            }
            if (host.getOid() == 3L || host.getOid() == 4L) {
                Assert.assertEquals(CLUSTER2_KEY, cpuProv.getCommodityType().getKey());
                Assert.assertEquals(CLUSTER2_KEY, memProv.getCommodityType().getKey());
                Assert.assertTrue(cluster.getCommoditySoldListList().stream().allMatch(cs -> CLUSTER2_KEY.equals(cs.getCommodityType().getKey())));
                assertCount++;
            }
            for (CommoditySoldDTO commoditySoldDTO : host.getCommoditySoldListList()) {
                if (commoditySoldDTO.getCommodityType().getType() == CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED_VALUE
                        || commoditySoldDTO.getCommodityType().getType() == CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE) {
                    Assert.assertTrue(commoditySoldDTO.getIsResold());
                } else {
                    Assert.assertFalse(commoditySoldDTO.getIsResold());
                }
            }
        }
        Assert.assertEquals(4, assertCount);
    }

    /**
     * When removeClusterCommBoughtGroupingOfHosts is called for the topology, the links between hosts and clusters
     * should be removed.
     */
    @Test
    public void testRemoveClusterCommBoughtGroupingOfHost() {
        Map<Long, TopologyEntityDTO> result = fakeEntityCreator.createFakeTopologyEntityDTOs(topologyDTOs, false, true);
        fakeEntityCreator.removeClusterCommBoughtGroupingOfHosts(topologyDTOs);
        for (TopologyEntityDTO host : topologyDTOs.values()) {
            Assert.assertEquals(0, host.getCommoditiesBoughtFromProvidersCount());
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
}