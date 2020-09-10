package com.vmturbo.plan.orchestrator.plan;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostMoles.BuyRIAnalysisServiceMole;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.AnalysisDTOMoles.AnalysisServiceMole;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.common.protobuf.utils.StringConstants;

public class PlanRpcServiceTest {

    private final AnalysisServiceMole testAnalysisRpcService = spy(new AnalysisServiceMole());
    private final BuyRIAnalysisServiceMole testBuyRiRpcService = spy(new BuyRIAnalysisServiceMole());
    private final GroupServiceMole testGroupRpcService = spy(new GroupServiceMole());
    private final RepositoryServiceMole testRepositoryRpcService = spy(new RepositoryServiceMole());
    private final long topologyId = 2222;
    // The  realtime topology context Id.
    private static final Long realtimeTopologyContextId = 777777L;

    //Runs tasks on same thread that's invoking execute/submit
    private ExecutorService sameThreadExecutor = MoreExecutors.newDirectExecutorService();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(testAnalysisRpcService,
                     testBuyRiRpcService, testGroupRpcService, testRepositoryRpcService);

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
            BuyRIAnalysisServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            PlanReservedInstanceServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            1, TimeUnit.SECONDS, realtimeTopologyContextId );
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
        final String resizeSettingName = ConfigurableActionSettings.Resize.getSettingName();
        final Setting resizeSetting = Setting.newBuilder().setSettingSpecName(resizeSettingName)
                .setEnumSettingValue(settingValue).build();
        return ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setSetting(resizeSetting).build())
                .build();
    }

}
