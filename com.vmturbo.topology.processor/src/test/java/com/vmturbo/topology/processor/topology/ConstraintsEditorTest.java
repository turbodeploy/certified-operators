package com.vmturbo.topology.processor.topology;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;

import io.grpc.stub.StreamObserver;

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
                        IgnoreConstraint.newBuilder().setCommodityType("ClusterCommodity")
                                .setGroupUuid(1l).build()
                )).build());
        final TopologyGraph graph = TopologyGraph.newGraph(topology);
        Assert.assertEquals(3, getActiveCommodities(graph).count());
        constraintsEditor.editConstraints(graph, changes);
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
        final TopologyGraph graph = TopologyGraph.newGraph(topology);
        Assert.assertEquals(4, getActiveCommodities(graph).count());
        constraintsEditor.editConstraints(graph, changes);
        Assert.assertEquals(0, getActiveCommodities(graph).count());
    }

    private ScenarioChange buildScenarioChange(@Nonnull String commodityType, long uuid) {
        return ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder().addIgnoreConstraints(
                        IgnoreConstraint.newBuilder().setCommodityType(commodityType)
                                .setGroupUuid(uuid).build()
                )).build();
    }

    private Stream<CommodityBoughtDTO> getActiveCommodities(TopologyGraph editedGraph) {
        return editedGraph.entities()
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .map(Builder::getCommoditiesBoughtFromProvidersList)
                    .flatMap(List::stream)
                    .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
                    .flatMap(List::stream)
                    .filter(CommodityBoughtDTO::getActive);
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
    }

    /**
     * Test implementation for GroupResolver
     */
    private static class TestGroupResolver extends GroupResolver {

        private TestGroupResolver() {
            super(new TopologyFilterFactory());
        }

        @Override
        public Set<Long> resolve(Group group, TopologyGraph topologyGraph) {
            return new HashSet<>(group.getGroup().getStaticGroupMembers().getStaticMemberOidsList());
        }
    }
}
