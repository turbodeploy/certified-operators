package com.vmturbo.plan.orchestrator.plan;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyRIAnalysisServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.utils.StringConstants;

public class PlanRpcServiceTest {

    private final AnalysisServiceMole testAnalysisRpcService = spy(new AnalysisServiceMole());
    private final BuyRIAnalysisServiceMole testBuyRiRpcService = spy(new BuyRIAnalysisServiceMole());

    //Runs tasks on same thread that's invoking execute/submit
    private ExecutorService sameThreadExecutor = MoreExecutors.newDirectExecutorService();

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
                                         sameThreadExecutor,
                                         mock(UserSessionContext.class),
                                         BuyRIAnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel()));
    }

    /**
     * Tests workflow of runQueuedPlan for run of OCP option 1: M2 + RI Buy.
     */
    @Test
    public void testRunQueuedPlanWithM2AndBuyRi() {
        PlanInstance planInstance = createOptimizePlanWithM2AndBuyRi();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(1)).startBuyRIAnalysis(any());
        // TODO: verify(testAnalysisRpcService, times(1)).startAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithM2AndBuyRi() {
        return PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build()))
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build())).build();
    }

    /**
     * Tests workflow of runQueuedPlan for run of OCP option 2: M2 with no RI Buy.
     */
    @Test
    public void testRunQueuedPlanWithM2AndNoBuyRi() {
        PlanInstance planInstance = createOptimizePlanWithM2AndNoBuyRi();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(0)).startBuyRIAnalysis(any());
        // TODO: verify(testAnalysisRpcService, times(1)).startAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithM2AndNoBuyRi() {
        return PlanInstance.newBuilder().setPlanId(2L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN).build())).build();
    }

    /**
     * Tests workflow of runQueuedPlan for run of plan option 3: RI Buy only using allocation demand.
     */
    @Test
    public void testRunQueuedPlanWithBuyRiOnly() {
        PlanInstance planInstance = createOptimizePlanWithBuyRiOnly();
        planService.runQueuedPlan(planInstance, response);
        verify(testBuyRiRpcService, times(1)).startBuyRIAnalysis(any());
    }

    private PlanInstance createOptimizePlanWithBuyRiOnly() {
        return PlanInstance.newBuilder().setPlanId(3L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                        .addChanges(getResizeScenarioChanges(StringConstants.DISABLED))
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build())))).build();
    }

    private ScenarioChange getResizeScenarioChanges(String actionSetting) {
        final EnumSettingValue settingValue = EnumSettingValue.newBuilder()
                .setValue(actionSetting).build();
        final String resizeSettingName = EntitySettingSpecs.Resize.getSettingName();
        final Setting resizeSetting = Setting.newBuilder().setSettingSpecName(resizeSettingName)
                .setEnumSettingValue(settingValue).build();
        return ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setSetting(resizeSetting).build())
                .build();
    }

}
