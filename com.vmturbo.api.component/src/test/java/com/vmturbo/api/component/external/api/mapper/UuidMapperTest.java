package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

public class UuidMapperTest {

    private GroupServiceMole groupServiceBackend = spy(GroupServiceMole.class);

    private PlanServiceMole planServiceMole = spy(PlanServiceMole.class);

    @Rule
    public GrpcTestServer grpcServer =
        GrpcTestServer.newServer(groupServiceBackend, planServiceMole);

    private UuidMapper uuidMapper;

    @Before
    public void setup() {
        uuidMapper = new UuidMapper(7L,
            PlanServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    @Test
    public void testRealtimeMarketId() {
        ApiId id = uuidMapper.fromUuid(UuidMapper.UI_REAL_TIME_MARKET_STR);
        assertTrue(id.isRealtimeMarket());
        assertEquals(7L, id.oid());
        assertEquals(UuidMapper.UI_REAL_TIME_MARKET_STR, id.uuid());
    }

    @Test
    public void testPlanId() {
        when(planServiceMole.getPlan(PlanId.newBuilder()
            .setPlanId(123)
            .build())).thenReturn(OptionalPlanInstance.newBuilder()
                .setPlanInstance(PlanInstance.newBuilder()
                    .setPlanId(123)
                    .setStatus(PlanStatus.READY))
                .build());

        ApiId id = uuidMapper.fromUuid("123");
        assertTrue(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any());

        // Test caching
        assertTrue(id.isPlan());
        verify(planServiceMole, times(1)).getPlan(any());
    }

    @Test
    public void testGroupId() {
        doReturn(GetGroupResponse.newBuilder()
            .setGroup(Group.newBuilder()
                .setId(123))
            .build()).when(groupServiceBackend).getGroup(GroupID.newBuilder()
                .setId(123)
                .build());

        ApiId id = uuidMapper.fromUuid("123");
        id.isGroup();
        assertTrue(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());

        // Test caching
        assertTrue(id.isGroup());
        verify(groupServiceBackend, times(1)).getGroup(any());
    }
}
