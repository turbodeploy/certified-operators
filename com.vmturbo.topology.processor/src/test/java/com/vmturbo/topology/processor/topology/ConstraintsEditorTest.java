package com.vmturbo.topology.processor.topology;

import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.buildTopologyEntityWithCommBought;
import static com.vmturbo.topology.processor.topology.TopologyEntityUtils.buildTopologyEntityWithCommSold;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.group.GroupConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.ConstraintGroup;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreEntityTypes;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.processor.group.GroupResolver;
import org.mockito.Mockito;

/**
 * Tests disabling of constraints
 */
public class ConstraintsEditorTest {

    private GroupServiceBlockingStub groupService;

    private GroupResolver groupResolver = new TestGroupResolver();

    private ConstraintsEditor constraintsEditor;

    @Before
    public void setup() throws IOException {
        constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
    }

    private static Group buildGroup(long id, List<Long> entities) {
        final GroupInfo groupInfo = GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder()
                        .addAllStaticMemberOids(entities).build()).build();
        return Group.newBuilder().setId(id).setGroup(groupInfo).build();
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, int type) {
        return TopologyEntityUtils.topologyEntityBuilder(
            TopologyEntityDTO.newBuilder().setOid(oid).addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder().addCommodityBought(
                    CommodityBoughtDTO.newBuilder().setCommodityType(
                        CommodityType.newBuilder().setType(type).setKey("").build()
                    ).setActive(true)
                )
            ));
    }

    @Nonnull
    private TopologyEntity.Builder buildTopologyEntity(long oid, EntityType entityType, int commodityType, String commodityKey) {
        CommodityType.Builder commodityTypeBuilder =
                CommodityType.newBuilder()
                        .setType(commodityType);
        if (commodityKey != null) {
            commodityTypeBuilder.setKey(commodityKey);
        }
        return TopologyEntityUtils.topologyEntityBuilder(
                TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setEntityType(entityType.getNumber())
                        .addCommoditiesBoughtFromProviders(
                                CommoditiesBoughtFromProvider.newBuilder().addCommodityBought(
                                        CommodityBoughtDTO.newBuilder().setCommodityType(
                                        commodityTypeBuilder.build())
                                                .setActive(true)
                                ).build()
                ));
    }

    @Test
    public void testIgnoreConstraint() throws IOException {
        final List<Group> groups = ImmutableList.of(buildGroup(1l, ImmutableList.of(1l, 2l)));
        final GroupTestService testService = new GroupTestService(groups);
        GrpcTestServer testServer = GrpcTestServer.newServer(testService);
        testServer.start();
        groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()));
        topology.put(3l, buildTopologyEntity(3l, CommodityDTO.CommodityType.NETWORK.getNumber()));
        List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder().addIgnoreConstraints(
                        IgnoreConstraint.newBuilder().setIgnoreGroup(
                                ConstraintGroup.newBuilder()
                                    .setCommodityType("ClusterCommodity")
                                    .setGroupUuid(1l).build())
                                .build()))
                .build());
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);
        Assert.assertEquals(3, getActiveCommodities(graph).count());
        constraintsEditor.editConstraints(graph, changes, false);
        Assert.assertEquals(1, getActiveCommodities(graph).count());
    }

    @Test
    public void testIgnoreConstraintTwoGroups() throws IOException {
        final List<Group> groups = ImmutableList.of(
                buildGroup(1l, ImmutableList.of(1l, 2l)),
                buildGroup(2l, ImmutableList.of(3l, 4l))
                );
        final GroupTestService testService = new GroupTestService(groups);
        GrpcTestServer testServer = GrpcTestServer.newServer(testService);
        testServer.start();
        groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, CommodityDTO.CommodityType.CLUSTER.getNumber()));
        topology.put(2l, buildTopologyEntity(2l, CommodityDTO.CommodityType.CLUSTER.getNumber()));
        topology.put(3l, buildTopologyEntity(3l, CommodityDTO.CommodityType.NETWORK.getNumber()));
        topology.put(4l, buildTopologyEntity(4l, CommodityDTO.CommodityType.NETWORK.getNumber()));
        List<ScenarioChange> changes = ImmutableList.of(
                buildScenarioChange("ClusterCommodity", 1l),
                buildScenarioChange("NetworkCommodity", 2l));
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);
        Assert.assertEquals(4, getActiveCommodities(graph).count());
        constraintsEditor.editConstraints(graph, changes, false);
        Assert.assertEquals(0, getActiveCommodities(graph).count());
    }

    @Test
    public void testIgnoreConstraintAllEntities() throws IOException {
        final List<Group> groups = ImmutableList.of(buildGroup(1l, ImmutableList.of(1l, 2l)));
        final GroupTestService testService = new GroupTestService(groups);
        GrpcTestServer testServer = GrpcTestServer.newServer(testService);
        testServer.start();
        groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, EntityType.PHYSICAL_MACHINE,
                CommodityDTO.CommodityType.CLUSTER.getNumber(), null));
        topology.put(2l, buildTopologyEntity(2l, EntityType.VIRTUAL_MACHINE,
                CommodityDTO.CommodityType.CPU.getNumber(), "cpu"));
        topology.put(3l, buildTopologyEntity(3l, EntityType.VIRTUAL_MACHINE,
                CommodityDTO.CommodityType.STORAGE.getNumber(), "storage"));
        List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder().addIgnoreConstraints(
                        IgnoreConstraint.newBuilder()
                                .setIgnoreAllEntities(true)
                                .build()))
                .build());
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);
        Assert.assertEquals(3, getActiveCommodities(graph).count());
        constraintsEditor.editConstraints(graph, changes, false);
        // As 2 out of the 3 commodities has a key, we should have only 2 commodities whose
        // active flag is set to false and 1 is set to true
        Assert.assertEquals(1, getActiveCommodities(graph).count());
    }
    @Test
    public void testIgnoreConstraintAllVMEntities() throws IOException {
        final List<Group> groups = ImmutableList.of(buildGroup(1l, ImmutableList.of(1l, 2l)));
        final GroupTestService testService = new GroupTestService(groups);
        GrpcTestServer testServer = GrpcTestServer.newServer(testService);
        testServer.start();
        groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();
        topology.put(1l, buildTopologyEntity(1l, EntityType.PHYSICAL_MACHINE,
                CommodityDTO.CommodityType.CLUSTER.getNumber(), null));
        topology.put(2l, buildTopologyEntity(2l, EntityType.VIRTUAL_MACHINE,
                CommodityDTO.CommodityType.CPU.getNumber(), "cpu"));
        topology.put(3l, buildTopologyEntity(3l, EntityType.VIRTUAL_MACHINE,
                CommodityDTO.CommodityType.DATASTORE.getNumber(), "datastore"));
        topology.put(4l, buildTopologyEntity(4l, EntityType.VIRTUAL_MACHINE,
                CommodityDTO.CommodityType.STORAGE.getNumber(), null));
        topology.put(5l, buildTopologyEntity(5l, EntityType.APPLICATION,
                CommodityDTO.CommodityType.VCPU.getNumber(), "vcpu"));
        List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder().addIgnoreConstraints(
                        IgnoreConstraint.newBuilder()
                                .setIgnoreEntityTypes(IgnoreEntityTypes.newBuilder()
                                        .addEntityTypes(EntityType.VIRTUAL_MACHINE)
                                        .build())
                                .build()))
                .build());
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);
        Assert.assertEquals(5, getActiveCommodities(graph).count());
        constraintsEditor.editConstraints(graph, changes, false);
        // As only 2 out of 3 VMs has a commodityKey, there should be only 2
        // commodities in the entire graph which have active set to false. The
        // remaining 3 entitues should have commoditbought as active.
        Assert.assertEquals(3, getActiveCommodities(graph).count());
    }

    @Test
    public void testEditConstraintsForAlleviatePressurePlan() throws IOException {
        final List<Group> groups = ImmutableList.of(buildGroup(1L, ImmutableList.of(1L, 3L, 4L, 5L, 6L)));
        final GroupTestService testService = new GroupTestService(groups);
        GrpcTestServer testServer = GrpcTestServer.newServer(testService);
        testServer.start();
        groupService = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        constraintsEditor = new ConstraintsEditor(groupResolver, groupService);
        final Map<Long, TopologyEntity.Builder> topology = new HashMap<>();

        // Set entities with commodities bought.
        topology.put(1L, buildTopologyEntityWithCommBought(1L, CommodityDTO.CommodityType.CLUSTER.getNumber(),
                        EntityType.PHYSICAL_MACHINE_VALUE, 1L));
        topology.put(2L, buildTopologyEntityWithCommBought(2L, CommodityDTO.CommodityType.CLUSTER.getNumber(),
                        EntityType.VIRTUAL_MACHINE_VALUE, 1L));

        // Set entities with commodities sold.
        topology.put(3L, buildTopologyEntityWithCommSold(3L, CommodityDTO.CommodityType.NETWORK.getNumber(),
                        EntityType.PHYSICAL_MACHINE_VALUE));
        topology.put(4L, buildTopologyEntityWithCommSold(4L, CommodityDTO.CommodityType.STORAGE_CLUSTER.getNumber(),
                        EntityType.PHYSICAL_MACHINE_VALUE));
        topology.put(5L, buildTopologyEntityWithCommSold(5L, CommodityDTO.CommodityType.CPU.getNumber(),
                        EntityType.PHYSICAL_MACHINE_VALUE));
        topology.put(6L, buildTopologyEntityWithCommSold(6L, CommodityDTO.CommodityType.DATACENTER.getNumber(),
                        EntityType.PHYSICAL_MACHINE_VALUE));

        // Set scenario change
        List<ScenarioChange> changes = ImmutableList.of(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder().addIgnoreConstraints(
                        IgnoreConstraint.newBuilder().setIgnoreGroup(
                                ConstraintGroup.newBuilder()
                                        .setCommodityType("ClusterCommodity")
                                        .setGroupUuid(1l).build())
                                .build()))
                .build());
        final TopologyGraph<TopologyEntity> graph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        // Pressure plan : disabled
        constraintsEditor.editConstraints(graph, changes, false);
        // Only PM's bought cluster commodity is disabled due to ScenarioChange.
        Assert.assertEquals(1, getActiveCommodities(graph).count());
        // All sold commodities are active.
        Assert.assertEquals(4, getActiveCommoditiesSold(graph).count());

        // Pressure plan : disabled
        // For alleviate pressure plan, VM's bought Cluster commodity should be disabled too.
        constraintsEditor.editConstraints(graph, changes, true);
        Assert.assertEquals(0, getActiveCommodities(graph).count());
        // And PM's : NETWORK, DATACENTER, STORAGE_CLUSTER sold is disabled because it is
        // required by pressure plan. Only CPU sold is active in this case.
        Assert.assertEquals(1, getActiveCommoditiesSold(graph).count());
    }

    private ScenarioChange buildScenarioChange(@Nonnull String commodityType, long uuid) {
        return ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder().addIgnoreConstraints(
                        IgnoreConstraint.newBuilder().setIgnoreGroup(
                                ConstraintGroup.newBuilder()
                                        .setCommodityType(commodityType)
                                        .setGroupUuid(uuid)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    private Stream<CommodityBoughtDTO> getActiveCommodities(TopologyGraph<TopologyEntity> editedGraph) {
        return editedGraph.entities()
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .map(Builder::getCommoditiesBoughtFromProvidersList)
                    .flatMap(List::stream)
                    .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                    .flatMap(List::stream)
                    .filter(CommodityBoughtDTO::getActive);
    }

    private Stream<CommoditySoldDTO> getActiveCommoditiesSold(TopologyGraph<TopologyEntity> editedGraph) {
        return editedGraph.entities()
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .map(Builder::getCommoditySoldListList)
                    .flatMap(List::stream)
                    .filter(CommoditySoldDTO::getActive);
    }

    /**
     * Test implementation of GroupServiceGrpc
     */
    private static class GroupTestService extends GroupServiceImplBase {

        private final List<Group> groups;

        private GroupTestService(List<Group> groups) {
            this.groups = groups;
        }

        @Override
        public void getGroup(GroupID request, StreamObserver<GetGroupResponse> responseObserver) {
            GetGroupResponse groupResponse = GetGroupResponse.newBuilder()
                    .setGroup(groups.stream()
                            .filter(group -> group.getId() == request.getId())
                            .findAny()
                            .get())
                    .build();
            responseObserver.onNext(groupResponse);
            responseObserver.onCompleted();
        }

        @Override
        public void getGroups(final GetGroupsRequest request, final StreamObserver<Group> responseObserver) {
            groups.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }

    /**
     * Test implementation for GroupResolver
     */
    private static class TestGroupResolver extends GroupResolver {

        private TestGroupResolver() {
            super(mock(SearchResolver.class), Mockito.mock(GroupConfig.class).groupServiceBlockingStub());
        }

        @Override
        public Set<Long> resolve(Group group, TopologyGraph<TopologyEntity> topologyGraph) {
            return new HashSet<>(group.getGroup().getStaticGroupMembers().getStaticMemberOidsList());
        }
    }
}
