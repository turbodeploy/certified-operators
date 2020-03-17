package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.Lists;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.topology.processor.api.TopologyProcessor;


public class UuidMapperTest {

    private GroupServiceMole groupServiceBackend = spy(GroupServiceMole.class);

    private PlanServiceMole planServiceMole = spy(PlanServiceMole.class);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private MagicScopeGateway magicScopeGateway = mock(MagicScopeGateway.class);

    private GroupExpander groupExpander = mock(GroupExpander.class);

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(groupServiceBackend, planServiceMole);

    private UuidMapper uuidMapper;

    @Before
    public void setup() throws OperationFailedException {
        uuidMapper = new UuidMapper(7L,
            magicScopeGateway,
            repositoryApi,
            topologyProcessor,
            PlanServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            groupExpander);

        when(magicScopeGateway.enter(any(String.class))).thenAnswer(invocation -> invocation.getArgumentAt(0, String.class));
    }

    @Test
    public void testRealtimeMarketId() throws OperationFailedException {
        ApiId id = uuidMapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR);
        assertTrue(id.isRealtimeMarket());
        assertEquals(7L, id.oid());
        assertEquals(UuidMapper.UI_REAL_TIME_MARKET_STR, id.uuid());

        assertFalse(id.isGroup());
        assertFalse(id.isEntity());
        assertFalse(id.isPlan());
    }

    @Test
    public void testSourceEntityId() {
        final long id = 17L;
        MinimalEntity minEntity = MinimalEntity.newBuilder()
            .setOid(id)
            .build();
        final SingleEntityRequest exceptionReq = ApiTestUtils.mockSingleEntityRequest(minEntity);
        when(exceptionReq.getMinimalEntity()).thenThrow(Status.INTERNAL.asRuntimeException());
        final SingleEntityRequest entityReq = ApiTestUtils.mockSingleEntityRequest(minEntity);

        when(repositoryApi.entityRequest(anyLong())).thenReturn(exceptionReq, entityReq);

        final ApiId apiId = uuidMapper.fromOid(id);
        assertFalse(apiId.isEntity());

        verify(repositoryApi, times(1)).entityRequest(id);

        assertTrue(apiId.isEntity());
        // No caching
        verify(repositoryApi, times(2)).entityRequest(id);


        assertFalse(apiId.isRealtimeMarket());
        assertFalse(apiId.isGroup());
        assertFalse(apiId.isPlan());
    }

    @Test
    public void testEntityIdException() {
        final long id = 17L;
        final SingleEntityRequest req = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
            .setOid(id)
            .build());
        when(repositoryApi.entityRequest(anyLong())).thenReturn(req);

        final ApiId apiId = uuidMapper.fromOid(id);
        assertTrue(apiId.isEntity());
        verify(repositoryApi, times(1)).entityRequest(id);

        // Test caching
        assertTrue(apiId.isEntity());
        verify(repositoryApi, times(1)).entityRequest(id);


        assertFalse(apiId.isRealtimeMarket());
        assertFalse(apiId.isGroup());
        assertFalse(apiId.isPlan());
    }

    @Test
    public void testProjectedEntityId() {
        // Negative ID for a projected entity.
        final long id = -17L;
        final SingleEntityRequest projReq = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
            .setOid(id)
            .build());
        when(repositoryApi.entityRequest(anyLong())).thenReturn(projReq);

        final ApiId apiId = uuidMapper.fromOid(id);
        assertTrue(apiId.isEntity());
        verify(repositoryApi, times(1)).entityRequest(id);
        // Verify we asked for the projected topology.
        verify(projReq).projectedTopology();

        // Test caching
        assertTrue(apiId.isEntity());
        verify(repositoryApi, times(1)).entityRequest(id);


        assertFalse(apiId.isRealtimeMarket());
        assertFalse(apiId.isGroup());
        assertFalse(apiId.isPlan());
    }

    @Test
    public void testPlanId() throws OperationFailedException {
        doReturn(OptionalPlanInstance.newBuilder()
            .setPlanInstance(PlanInstance.newBuilder()
                .setPlanId(123)
                .setStatus(PlanStatus.READY))
            .build()).when(planServiceMole).getPlan(PlanId.newBuilder()
                .setPlanId(123)
                .build());

        ApiId id = uuidMapper.fromUuid("123");
        assertTrue(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any());

        // Test caching
        assertTrue(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any());

        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isGroup());
        assertFalse(id.isEntity());
    }

    @Test
    public void testPlanIdNotPlan() throws OperationFailedException {
        doReturn(OptionalPlanInstance.getDefaultInstance())
            .when(planServiceMole).getPlan(PlanId.newBuilder()
                .setPlanId(123)
                .build());

        ApiId id = uuidMapper.fromUuid("123");
        assertFalse(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any());

        // Test caching
        assertFalse(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any());
    }

    @Test
    public void testPlanIdException() throws OperationFailedException {
        final PlanId plan = PlanId.newBuilder()
            .setPlanId(123)
            .build();
        doReturn(Optional.of(Status.INTERNAL.asException())).when(planServiceMole).getPlanError(plan);

        ApiId id = uuidMapper.fromUuid("123");
        assertFalse(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any(), any());

        doReturn(Optional.empty()).when(planServiceMole).getPlanError(plan);
        doReturn(OptionalPlanInstance.newBuilder()
            .setPlanInstance(PlanInstance.newBuilder()
                .setPlanId(123)
                .setStatus(PlanStatus.READY))
            .build()).when(planServiceMole).getPlan(plan);

        // No caching of the error!
        assertTrue(id.isPlan());
        verify(planServiceMole, times(2)).getPlan(any(), any());

        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isGroup());
        assertFalse(id.isEntity());
    }

    @Test
    public void testGroupId() throws OperationFailedException {

        when(groupExpander.getMembersForGroup(any())).thenReturn(
            ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Collections.emptyList()).entities(Collections.emptyList()).build());
        final MinimalEntity vmEntity = MinimalEntity.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(456)
                .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(vmEntity));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                        .setId(123)
                        .addExpectedTypes(MemberType.newBuilder()
                                        .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                            .setType(GroupType.REGULAR)
                            .setDisplayName("foo")
                            .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                    .setType(MemberType.newBuilder()
                                        .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))))
                            )
                .build())
            .build()).when(groupServiceBackend).getGroup(GroupID.newBuilder()
                .setId(123)
                .build());

        ApiId id = uuidMapper.fromUuid("123");
        assertTrue(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());

        // Test caching
        assertTrue(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());

        assertThat(id.getCachedGroupInfo().get().getEntityTypes(), contains(UIEntityType.VIRTUAL_MACHINE));
        assertThat(id.getCachedGroupInfo().get().isGlobalTempGroup(), is(false));
        assertThat(id.getDisplayName(), is("foo"));
        assertThat(id.getScopeTypes().get(), contains(UIEntityType.VIRTUAL_MACHINE));

        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isPlan());
        assertFalse(id.isEntity());
    }

    /**
     * Test that cloud tempgroups are accepted as cloud, group, and cloud-group ApiIds.
     * @throws Exception never.
     */
    @Test
    public void testIsCloudGroup() throws Exception {
        when(groupExpander.getMembersForGroup(any())).thenReturn(
            ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Collections.emptyList()).entities(Collections.emptyList()).build());
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                .setId(123)
                .addExpectedTypes(MemberType.newBuilder()
                    .setEntity(UIEntityType.VIRTUAL_VOLUME.typeNumber()))
                .setDefinition(GroupDefinition.newBuilder()
                    .setIsTemporary(true)
                    .setDisplayName("foo")
                    .setOptimizationMetadata(GroupDefinition.OptimizationMetadata.newBuilder()
                        .setEnvironmentType(EnvironmentType.CLOUD))
                )
                .build())
            .build()).when(groupServiceBackend).getGroup(GroupID.newBuilder()
            .setId(123)
            .build());

        ApiId id = uuidMapper.fromUuid("123");
        assertTrue(id.isCloudGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());

        // Test caching
        assertTrue(id.isCloudGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());

        assertTrue(id.isCloud());
        assertTrue(id.isGroup());
        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isPlan());
        assertFalse(id.isEntity());
    }

    /**
     * Test that cloud entities and temp-groups are accepted as cloud ApiIds, and that others aren't.
     * @throws Exception never
     */
    @Test
    public void testIsCloud() throws Exception {
        when(groupExpander.getMembersForGroup(any())).thenReturn(
            ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Collections.emptyList()).entities(Collections.emptyList()).build());
        final MultiEntityRequest req0 = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(req0);

        final SingleEntityRequest req1 = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
            .setOid(123)
            .build());
        when(repositoryApi.entityRequest(123)).thenReturn(req1);

        final ApiId apiId1 = uuidMapper.fromOid(123);
        assertTrue(apiId1.isEntity());
        assertFalse(apiId1.isCloudEntity());
        assertFalse(apiId1.isCloud());

        final SingleEntityRequest req2 = ApiTestUtils.mockSingleEntityRequest(MinimalEntity.newBuilder()
            .setOid(456)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build());
        when(repositoryApi.entityRequest(456)).thenReturn(req2);

        final ApiId apiId2 = uuidMapper.fromOid(456);
        assertTrue(apiId2.isEntity());
        assertTrue(apiId2.isCloudEntity());
        assertTrue(apiId2.isCloud());

        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                .setId(789)
                .addExpectedTypes(MemberType.newBuilder()
                    .setEntity(UIEntityType.VIRTUAL_VOLUME.typeNumber()))
                .setDefinition(GroupDefinition.newBuilder()
                    .setIsTemporary(true)
                    .setDisplayName("foo")
                    .setOptimizationMetadata(GroupDefinition.OptimizationMetadata.newBuilder()
                        .setEnvironmentType(EnvironmentType.CLOUD))
                )
                .build())
            .build()).when(groupServiceBackend).getGroup(GroupID.newBuilder()
            .setId(789)
            .build());

        ApiId apiId3 = uuidMapper.fromUuid("789");
        assertTrue(apiId3.isGroup());
        assertTrue(apiId3.isCloudGroup());
        assertTrue(apiId3.isCloud());

        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                    .setId(13579)
                    .addExpectedTypes(MemberType.newBuilder()
                        .setEntity(UIEntityType.VIRTUAL_VOLUME.typeNumber()))
                    .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName("bar")
                        .setOptimizationMetadata(GroupDefinition.OptimizationMetadata.newBuilder()
                            .setEnvironmentType(EnvironmentType.CLOUD))
                    )
                    .build())
            .build()).when(groupServiceBackend).getGroup(GroupID.newBuilder()
            .setId(13579)
            .build());

        ApiId apiId4 = uuidMapper.fromUuid("13579");
        assertFalse(apiId4.isCloudGroup());
        assertTrue(apiId4.isGroup());
        assertFalse(apiId4.isCloud());
    }


    @Test
    public void testGroupIdNotGroup() throws OperationFailedException {
        doReturn(GetGroupResponse.getDefaultInstance())
                .when(groupServiceBackend).getGroup(GroupID.newBuilder()
                .setId(123)
                .build());

        ApiId id = uuidMapper.fromUuid("123");
        assertFalse(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());

        // Test caching
        assertFalse(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());
    }

    @Test
    public void testGroupIdError() throws OperationFailedException {

        when(groupExpander.getMembersForGroup(any())).thenReturn(
            ImmutableGroupAndMembers.builder().group(Grouping.newBuilder().build())
                .members(Collections.emptyList()).entities(Collections.emptyList()).build());
        final MinimalEntity vmEntity = MinimalEntity.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(456)
                .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiMinEntityReq(Lists.newArrayList(vmEntity));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        GroupID groupID = GroupID.newBuilder()
            .setId(123)
            .build();
        doReturn(Optional.of(Status.INTERNAL.asException()))
            .when(groupServiceBackend).getGroupError(groupID);

        ApiId id = uuidMapper.fromUuid("123");
        assertFalse(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any(), any());

        // No more error.
        doReturn(Optional.empty())
            .when(groupServiceBackend).getGroupError(groupID);
        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Grouping.newBuilder()
                            .setId(123)
                            .addExpectedTypes(MemberType.newBuilder()
                                            .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                            .setDefinition(GroupDefinition.newBuilder()
                                .setType(GroupType.REGULAR)
                                .setDisplayName("foo")
                                .setStaticGroupMembers(StaticMembers.newBuilder()
                                    .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                            .setEntity(UIEntityType.VIRTUAL_MACHINE.typeNumber()))))
                                )
                    .build())
            .build()).when(groupServiceBackend).getGroup(groupID);

        // Test no caching of error.
        assertTrue(id.isGroup());
        verify(groupServiceBackend, times(2)).getGroup(any(), any());

        assertThat(id.getCachedGroupInfo().get().getEntityTypes(), contains(UIEntityType.VIRTUAL_MACHINE));
        assertThat(id.getCachedGroupInfo().get().isGlobalTempGroup(), is(false));
        assertThat(id.getDisplayName(), is("foo"));

        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isPlan());
        assertFalse(id.isEntity());
    }
}
