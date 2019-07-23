package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
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

import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Status;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
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

public class UuidMapperTest {

    private GroupServiceMole groupServiceBackend = spy(GroupServiceMole.class);

    private PlanServiceMole planServiceMole = spy(PlanServiceMole.class);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(groupServiceBackend, planServiceMole);

    private UuidMapper uuidMapper;

    @Before
    public void setup() {
        uuidMapper = new UuidMapper(7L,
            repositoryApi,
            PlanServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    @Test
    public void testRealtimeMarketId() {
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
    public void testPlanId() {
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
    public void testPlanIdNotPlan() {
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
    public void testPlanIdException() {
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
    public void testGroupId() {
        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Group.newBuilder()
                .setId(123)
                .setGroup(GroupInfo.newBuilder()
                    .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
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

        assertThat(id.getCachedGroupInfo().get().getEntityType(), is(UIEntityType.VIRTUAL_MACHINE));
        assertThat(id.getCachedGroupInfo().get().isGlobalTempGroup(), is(false));

        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isPlan());
        assertFalse(id.isEntity());
    }

    @Test
    public void testGroupIdNotGroup() {
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
    public void testGroupIdError() {
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
            .setGroup(Group.newBuilder()
                .setId(123)
                .setGroup(GroupInfo.newBuilder()
                    .setEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .build())
            .build()).when(groupServiceBackend).getGroup(groupID);

        // Test no caching of error.
        assertTrue(id.isGroup());
        verify(groupServiceBackend, times(2)).getGroup(any(), any());

        assertThat(id.getCachedGroupInfo().get().getEntityType(), is(UIEntityType.VIRTUAL_MACHINE));
        assertThat(id.getCachedGroupInfo().get().isGlobalTempGroup(), is(false));

        assertFalse(id.isRealtimeMarket());
        assertFalse(id.isPlan());
        assertFalse(id.isEntity());
    }
}
