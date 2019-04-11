package com.vmturbo.plan.orchestrator.plan;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyRIAnalysisServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;

import io.grpc.stub.StreamObserver;

public class PlanRpcServiceTest {

    private final AnalysisServiceMole testAnalysisRpcService = spy(new AnalysisServiceMole());
    private final BuyRIAnalysisServiceMole testBuyRiRpcService = spy(new BuyRIAnalysisServiceMole());
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testAnalysisRpcService,
                     testBuyRiRpcService);

    private PlanRpcService planService;
    @SuppressWarnings("unchecked")
    private StreamObserver<PlanInstance> response = mock(StreamObserver.class);
 
    @Before
    public void setup() {
        planService = new PlanRpcService(mock(PlanDao.class),
                                         AnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                                         mock(PlanNotificationSender.class),
                                         mock(ExecutorService.class),
                                         mock(UserSessionContext.class),
                                         BuyRIAnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }
    @Test
    public void testRunQueuedPlanWithBuyRi() {
        PlanInstance planInstance = createOptimizePlanWithBuyRi();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(1)).startBuyRIAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithBuyRi() {
        return PlanInstance.newBuilder().setPlanId(1l).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN_TYPE)
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build())))).build();
    }
}
