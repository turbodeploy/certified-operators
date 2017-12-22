package com.vmturbo.plan.orchestrator.reservation;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;

/**
 * Test cases for {@link ReservationRpcService}.
 */
public class ReservationRpcServiceTest {

    private PlanDao planDao;

    private PlanRpcService planRpcService;

    private ReservationRpcService reservationRpcService;

    @Before
    public void setup() {
        planDao = Mockito.mock(PlanDao.class);
        planRpcService = Mockito.mock(PlanRpcService.class);
        reservationRpcService = new ReservationRpcService(planDao, planRpcService);
    }

    @Test
    public void testInitialPlacement() throws Exception {
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                .addChanges(ScenarioChange.newBuilder()
                        .setTopologyAddition(TopologyAddition.newBuilder()
                                .setTemplateId(123L)))
                .build();
        final InitialPlacementRequest request = InitialPlacementRequest.newBuilder()
                .setScenarioInfo(scenarioInfo)
                .build();

        final StreamObserver<InitialPlacementResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        final PlanInstance planInstance = PlanInstance.newBuilder()
                .setPlanId(1L)
                .setStatus(PlanStatus.RUNNING_ANALYSIS)
                .build();
        Mockito.when(planDao.createPlanInstance(Mockito.any(), Mockito.any())).thenReturn(planInstance);
        reservationRpcService.initialPlacement(request, mockObserver);

        Mockito.verify(planDao, Mockito.times(1))
                .createPlanInstance(Mockito.any(), Mockito.any());
        Mockito.verify(planRpcService, Mockito.times(1))
                .runPlan(Mockito.any(), Mockito.any());
        Mockito.verify(mockObserver).onNext(InitialPlacementResponse.newBuilder()
                .setPlanId(1L)
                .build());
        Mockito.verify(mockObserver).onCompleted();
    }
}
