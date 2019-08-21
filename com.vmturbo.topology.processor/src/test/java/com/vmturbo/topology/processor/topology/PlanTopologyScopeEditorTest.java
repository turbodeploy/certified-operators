package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

public class PlanTopologyScopeEditorTest {
    private GroupResolver groupResolver = mock(GroupResolver.class);
    private PlanTopologyScopeEditor planTopologyScopeEditor;
    private GroupServiceMole groupServiceClient = spy(new GroupServiceMole());

    TopologyEntity.Builder da1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.DISK_ARRAY_VALUE)
                                .setOid(50001L)
                                .setDisplayName("da1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l))));
    TopologyEntity.Builder st1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.STORAGE_VALUE)
                                .setOid(40001L)
                                .setDisplayName("st1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(da1.getOid())));
    TopologyEntity.Builder st2 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.STORAGE_VALUE)
                                .setOid(40002L)
                                .setDisplayName("st2")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l))));
    TopologyEntity.Builder dc1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.DATACENTER_VALUE)
                                .setOid(10001L)
                                .setDisplayName("dc1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l))));
    TopologyEntity.Builder dc2 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.DATACENTER_VALUE)
                                .setOid(10002L)
                                .setDisplayName("dc2")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l))));
    TopologyEntity.Builder pm1InDC1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setOid(20001L)
                                .setDisplayName("pm1InDC1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(dc1.getOid())));
    TopologyEntity.Builder pm2InDC1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setOid(20002L)
                                .setDisplayName("pm2InDC1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(dc1.getOid())));
    TopologyEntity.Builder pmInDC2 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                                .setOid(20003L)
                                .setDisplayName("pmInDC2")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                         .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                         .newBuilder().setProviderId(dc2.getOid())));
    TopologyEntity.Builder vm1InDC1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setOid(30001L)
                                .setDisplayName("vm1InDC1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(pm1InDC1.getOid()))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(st1.getOid()))
                                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                        .setConnectedEntityId(100001L)
                                        .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)));
    TopologyEntity.Builder vm2InDC1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setOid(30002L)
                                .setDisplayName("vm2InDC1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(pm2InDC1.getOid()))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(st1.getOid())));
    TopologyEntity.Builder vmInDC2 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                .setOid(30003L)
                                .setDisplayName("vmInDC2")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(pmInDC2.getOid()))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(st2.getOid())));
    TopologyEntity.Builder app1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.APPLICATION_VALUE)
                                .setOid(60001L)
                                .setDisplayName("app1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(vm1InDC1.getOid())));
    TopologyEntity.Builder as1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.APPLICATION_SERVER_VALUE)
                                .setOid(70001L)
                                .setDisplayName("as1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(vm1InDC1.getOid())));
    TopologyEntity.Builder as2 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.APPLICATION_SERVER_VALUE)
                                .setOid(70002L)
                                .setDisplayName("as2")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(vmInDC2.getOid())));
    TopologyEntity.Builder ba1 = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.BUSINESS_APPLICATION_VALUE)
                                .setOid(80001L)
                                .setDisplayName("ba1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(as1.getOid()))
                                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                        .newBuilder().setProviderId(as2.getOid())));
    TopologyEntity.Builder virtualVolume = TopologyEntity
                    .newBuilder(TopologyEntityDTO.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                                .setOid(100001L)
                                .setDisplayName("vv1")
                                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                        .newBuilder().addDiscoveringTargetIds(1l)))
                                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                        .setConnectedEntityId(30001L)
                                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)));
    ImmutableMap<Long, TopologyEntity.Builder> topologyEntitiesMap = ImmutableMap.<Long, TopologyEntity.Builder>builder()
                    .put(app1.getOid(), app1)
                    .put(vm1InDC1.getOid(), vm1InDC1)
                    .put(vm2InDC1.getOid(), vm2InDC1)
                    .put(vmInDC2.getOid(), vmInDC2)
                    .put(pm1InDC1.getOid(), pm1InDC1)
                    .put(pm2InDC1.getOid(), pm2InDC1)
                    .put(pmInDC2.getOid(), pmInDC2)
                    .put(dc1.getOid(), dc1)
                    .put(dc2.getOid(), dc2)
                    .put(st1.getOid(), st1)
                    .put(st2.getOid(), st2)
                    .put(da1.getOid(), da1)
                    .put(ba1.getOid(), ba1)
                    .put(as1.getOid(), as1)
                    .put(as2.getOid(), as2)
                    .put(virtualVolume.getOid(), virtualVolume)
                    .build();
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceClient);

    @Before
    public void setup() {
        planTopologyScopeEditor = new PlanTopologyScopeEditor(GroupServiceGrpc
                .newBlockingStub(grpcServer.getChannel()));
    }
    @Test
    public void testScopeCloudTopology() {
    // creating a cloud topology with region London and region Ohio
    // London owns az1London and az2LLondon, Ohio owns azOhio,
    // computeTier connected to London and Ohio
    // vmInLondon connected to az1London, ba owns vmInLondon
        TopologyEntity.Builder az1London = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setOid(10001L)
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder az2London = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setOid(10002L)
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder azOhio = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setOid(10003L)
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder regionLondon = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.REGION_VALUE)
                        .setOid(20001L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(10001L)
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(10002L)
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder regionOhio = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.REGION_VALUE)
                        .setOid(20002L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(10003L)
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder vmInLondon = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(40001L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(10001L)
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder computeTier = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .setOid(30001L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(20001L)
                                .setConnectedEntityType(EntityType.REGION_VALUE))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(20002L)
                                .setConnectedEntityType(EntityType.REGION_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder ba = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setOid(50001L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(40001L)
                                .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder vmInOhio = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(40002L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(10003L)
                                .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(60001L)
                                .setConnectedEntityType(EntityType.VIRTUAL_VOLUME_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder storageTier = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setOid(70001L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(20001L)
                                .setConnectedEntityType(EntityType.REGION_VALUE))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(20002L)
                                .setConnectedEntityType(EntityType.REGION_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        TopologyEntity.Builder virtualVolumeInOhio = TopologyEntity
                .newBuilder(TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setOid(60001L)
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(10003L).setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE))
                        .addConnectedEntityList(ConnectedEntity.newBuilder()
                                .setConnectedEntityId(70001L).setConnectedEntityType(EntityType.STORAGE_TIER_VALUE))
                        .setOrigin(Origin.newBuilder().setDiscoveryOrigin(DiscoveryOrigin
                                .newBuilder().addDiscoveringTargetIds(1l))));
        Map<Long, TopologyEntity.Builder> topologyEntitiesMap = new HashMap<>();
        topologyEntitiesMap.put(az1London.getOid(), az1London);
        topologyEntitiesMap.put(az2London.getOid(), az2London);
        topologyEntitiesMap.put(azOhio.getOid(), azOhio);
        topologyEntitiesMap.put(regionLondon.getOid(), regionLondon);
        topologyEntitiesMap.put(regionOhio.getOid(), regionOhio);
        topologyEntitiesMap.put(computeTier.getOid(), computeTier);
        topologyEntitiesMap.put(vmInLondon.getOid(), vmInLondon);
        topologyEntitiesMap.put(vmInOhio.getOid(), vmInOhio);
        topologyEntitiesMap.put(ba.getOid(), ba);
        topologyEntitiesMap.put(virtualVolumeInOhio.getOid(), virtualVolumeInOhio);
        topologyEntitiesMap.put(storageTier.getOid(), storageTier);
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Region")
                                .setScopeObjectOid(20001L).setDisplayName("London").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeCloudTopology(graph, planScope);
        assertTrue(result.size()==7);
        assertTrue(result.getEntity(az1London.getOid()).isPresent());
        assertTrue(result.getEntity(az2London.getOid()).isPresent());
        assertTrue(result.getEntity(regionLondon.getOid()).isPresent());
        assertTrue(result.getEntity(computeTier.getOid()).isPresent());
        assertTrue(result.getEntity(vmInLondon.getOid()).isPresent());
        assertTrue(result.getEntity(ba.getOid()).isPresent());
        assertTrue(result.getEntity(storageTier.getOid()).isPresent());
        assertFalse(result.getEntity(virtualVolumeInOhio.getOid()).isPresent());
        assertFalse(result.getEntity(vmInOhio.getOid()).isPresent());
        assertFalse(result.getEntity(azOhio.getOid()).isPresent());
        assertFalse(result.getEntity(regionOhio.getOid()).isPresent());
    }

    @Test
    public void testScopeOnpremTopologyOnCluster() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);
        Group g = Group.newBuilder().setCluster(ClusterInfo.newBuilder()
                .setMembers(StaticGroupMembers.newBuilder().addStaticMemberOids(pm1InDC1.getOid())
                        .addStaticMemberOids(pm2InDC1.getOid()))).build();
        List<Group> groups = Arrays.asList(g);
        when(groupServiceClient.getGroups(GetGroupsRequest.newBuilder().addId(90001L)
                .setResolveClusterSearchFilters(true).build())).thenReturn(groups);
        when(groupResolver.resolve(eq(g), eq(graph))).thenReturn(new HashSet<>(Arrays.asList(pm1InDC1.getOid(), pm2InDC1.getOid())));

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Cluster")
                                .setScopeObjectOid(90001L).setDisplayName("PM cluster/DC1").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, graph, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        assertTrue(result.size() == 11);
    }

    @Test
    public void testScopeOnpremTopologyOnBA() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("BusinessApplication")
                                .setScopeObjectOid(80001L).setDisplayName("BusinessApplication1").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, graph, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        assertTrue(result.size() == 16);
    }

    @Test
    public void testScopeOnpremTopologyOnStorage() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Storage")
                                .setScopeObjectOid(40002L).setDisplayName("Storage2").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, graph, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertTrue(result.size() == 6);
    }

    @Test
    public void testScopeOnpremTopologyOnVM() throws PipelineStageException {
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
        TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topologyEntitiesMap);

        final PlanScope planScope = PlanScope.newBuilder()
                        .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("VirtualMachine")
                                .setScopeObjectOid(30002L).setDisplayName("VM2").build()).build();
        TopologyGraph<TopologyEntity> result = planTopologyScopeEditor
                .scopeOnPremTopology(topologyInfo, graph, planScope, groupResolver,  new ArrayList<ScenarioChange>());
        result.entities().forEach(e -> System.out.println(e.getOid() + " "));
        assertTrue(result.size() == 11);
    }
}
