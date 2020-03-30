package com.vmturbo.topology.processor.group.policy;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.neverDiscoveredTopologyEntity;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.topologyEntity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

/**
 * Topology Graph for tests:
 * VM5        VM6
 * |   \      |    \
 * VDC9  \     VDC10 \
 * |     \    |      \
 * PM1 - ST3  PM2 -- ST4
 * \          |
 * DC7        DC8
 *
 * <p>PM1 sells network 20, 21.
 * PM2 sells network 22, 23.
 */
public class ReservationPolicyFactoryTest {

    private final GroupServiceMole groupServiceMole = Mockito.spy(new GroupServiceMole());

    private ReservationPolicyFactory reservationPolicyFactory;

    private TopologyGraph<TopologyEntity> topologyGraph;

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceMole);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        final Map<Long, TopologyEntity.Builder> topologyMap = new HashMap<>();


        topologyMap.put(20L, createNetwork(20L, "Network"));
        topologyMap.put(21L, createNetwork(21L, "Other"));
        topologyMap.put(22L, createNetwork(22L, "My Network"));
        topologyMap.put(23L, createNetwork(23L, "Network of Mine"));

        topologyMap.put(3L, topologyEntity(3L, EntityType.STORAGE));
        topologyMap.put(7L, topologyEntity(7L, EntityType.DATACENTER));
        topologyMap.put(1L, topologyEntity(1L, EntityType.PHYSICAL_MACHINE, 3, 7));
        addNetworkConnection(topologyMap.get(1L), topologyMap.get(20L));
        addNetworkConnection(topologyMap.get(1L), topologyMap.get(21L));
        topologyMap.put(9L, topologyEntity(9L, EntityType.VIRTUAL_DATACENTER, 1));
        topologyMap.put(5L, neverDiscoveredTopologyEntity(5L, EntityType.VIRTUAL_MACHINE, 9, 3));

        topologyMap.put(4L, topologyEntity(4L, EntityType.STORAGE));
        topologyMap.put(8L, topologyEntity(8L, EntityType.DATACENTER));
        topologyMap.put(2L, topologyEntity(2L, EntityType.PHYSICAL_MACHINE, 4, 8));
        addNetworkConnection(topologyMap.get(2L), topologyMap.get(22L));
        addNetworkConnection(topologyMap.get(2L), topologyMap.get(23L));
        topologyMap.put(10L, topologyEntity(10L, EntityType.VIRTUAL_DATACENTER, 2));
        topologyMap.put(6L, topologyEntity(6L, EntityType.VIRTUAL_MACHINE, 10, 4));

        reservationPolicyFactory = new ReservationPolicyFactory(
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topologyMap);
    }

    @Test
    public void testGeneratePolicy() {
        final ReservationConstraintInfo clusterConstraint = ReservationConstraintInfo.newBuilder()
                .setConstraintId(123L)
                .setType(ReservationConstraintInfo.Type.CLUSTER)
                .build();
        final GetMembersRequest request = GetMembersRequest.newBuilder().addId(123L).build();
        final GetMembersResponse response = GetMembersResponse.newBuilder()
                .setGroupId(123L)
                .addAllMemberId(Lists.newArrayList(1L, 3L))
                .build();
        Mockito.when(groupServiceMole.getMembers(request))
                .thenReturn(Collections.singletonList(response));
        final PlacementPolicy placementPolicy =
                reservationPolicyFactory.generatePolicyForInitialPlacement(topologyGraph,
                        Lists.newArrayList(clusterConstraint), Sets.newHashSet(5L));
        final Policy policy = placementPolicy.getPolicyDefinition();
        Mockito.verify(groupServiceMole, Mockito.times(1)).getMembers(request);
        Assert.assertTrue(policy.getPolicyInfo().getEnabled());
        Assert.assertTrue(policy.getPolicyInfo().hasBindToGroup());
    }

    @Test
    public void testGenerateProviderMembersDataCenter() {
        final ReservationConstraintInfo dataCenterConstraint =
                ReservationConstraintInfo.newBuilder()
                        .setConstraintId(7L)
                        .setType(ReservationConstraintInfo.Type.DATA_CENTER)
                        .build();
        final Map<Integer, Set<TopologyEntity>> entityMap =
                reservationPolicyFactory.getProviderMembersOfConstraint(topologyGraph,
                        dataCenterConstraint);
        Assert.assertEquals(1L, entityMap.size());
        Assert.assertEquals(1L, entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).size());
        Assert.assertEquals(1L,
                entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).iterator().next().getOid());
    }

    @Test
    public void testGenerateProviderMembersVDC() {
        final ReservationConstraintInfo vdcConstraint = ReservationConstraintInfo.newBuilder()
                .setConstraintId(9L)
                .setType(ReservationConstraintInfo.Type.VIRTUAL_DATA_CENTER)
                .build();
        final Map<Integer, Set<TopologyEntity>> entityMap =
                reservationPolicyFactory.getProviderMembersOfConstraint(topologyGraph,
                        vdcConstraint);
        Assert.assertEquals(1L, entityMap.size());
        Assert.assertEquals(1L, entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).size());
        Assert.assertEquals(1L,
                entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).iterator().next().getOid());
    }

    /**
     * Test that the factory generates the correct providers when given a network constraint.
     */
    @Test
    public void testGenerateProviderMembersNetwork() {
        final ReservationConstraintInfo vdcConstraint = ReservationConstraintInfo.newBuilder()
            // The "Other" network.
            .setConstraintId(21)
            .setType(Type.NETWORK)
            .build();
        final Map<Integer, Set<TopologyEntity>> entityMap =
            reservationPolicyFactory.getProviderMembersOfConstraint(topologyGraph,
                vdcConstraint);
        Assert.assertEquals(1L, entityMap.size());
        Assert.assertEquals(1L, entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).size());
        // Only PM 1 is connected to the other network.
        Assert.assertEquals(1L,
            entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).iterator().next().getOid());
    }

    /**
     * Test that the factory uses an exact name match when looking for hosts that are related to
     * a network.
     */
    @Test
    public void testGenerateProviderMembersNetworkExactMatch() {
        final ReservationConstraintInfo vdcConstraint = ReservationConstraintInfo.newBuilder()
            // The "Network" network.
            .setConstraintId(20)
            .setType(Type.NETWORK)
            .build();
        final Map<Integer, Set<TopologyEntity>> entityMap =
            reservationPolicyFactory.getProviderMembersOfConstraint(topologyGraph,
                vdcConstraint);
        Assert.assertEquals(1L, entityMap.size());
        Assert.assertEquals(1L, entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).size());
        // Only PM 1 is connected to the other network.
        // PM2 is connected to networks that include "Network" in the name.
        Assert.assertEquals(1L,
            entityMap.get(EntityType.PHYSICAL_MACHINE_VALUE).iterator().next().getOid());
    }


    private TopologyEntity.Builder createNetwork(final long oid, final String displayName) {
        TopologyEntity.Builder network = topologyEntity(oid, EntityType.NETWORK);
        network.getEntityBuilder().setDisplayName(displayName);
        return network;
    }

    private TopologyEntity.Builder addNetworkConnection(final TopologyEntity.Builder host,
                                                        final TopologyEntity.Builder network) {
        host.getEntityBuilder().addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NETWORK_VALUE)
                .setKey("Network::" + network.getDisplayName())));
        return host;
    }
}
