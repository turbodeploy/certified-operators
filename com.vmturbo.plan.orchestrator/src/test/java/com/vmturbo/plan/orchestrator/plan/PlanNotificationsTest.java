package com.vmturbo.plan.orchestrator.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.internal.matchers.CapturesArguments;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.plan.orchestrator.api.PlanListener;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.reservation.ReservationPlacementHandler;

/**
 * Test is aimed to cover plan orchestrator API classes.
 */
public class PlanNotificationsTest {

    private static final long TIMEOUT = 30000;
    private static final long TOPOLOGY_ID = 1234L;
    private static final long ACT_PLAN_ID = 3456L;

    @Rule
    public TestName testName = new TestName();

    private IntegrationTestServer server;

    private PlanOrchestratorClientImpl client;

    private ExecutorService threadPool;

    private PlanDao planDao;

    private ReservationPlacementHandler reservationPlacementHandler;

    private PlanProgressListener actionsListener;

    private IMessageReceiver<PlanInstance> messageReceiver;

    @Before
    public void init() throws Exception {
        server = new IntegrationTestServer(testName, PlanTestConfig.class);
        threadPool = Executors.newCachedThreadPool();
        messageReceiver = server.getBean("messageChannel");
        client = new PlanOrchestratorClientImpl(messageReceiver, threadPool, 0);
        planDao = server.getBean(PlanDao.class);
        actionsListener = server.getBean(PlanProgressListener.class);
        reservationPlacementHandler = server.getBean(ReservationPlacementHandler.class);
    }

    @After
    public void shutdown() throws Exception {
        server.close();
        threadPool.shutdownNow();
    }

    /**
     * Verify that resizeEnabled is calculated correctly based on ScenarioChanges.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testResizeEnabled() throws Exception {
        PlanInstance planInstance = PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                        .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build())))).build();
        Assert.assertTrue(PlanRpcServiceUtil.isScalingEnabled(planInstance.getScenario().getScenarioInfo()));
    }

    /**
     * Verify that resizeEnabled is calculated correctly based on ScenarioChanges.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testResizeDisabled() throws Exception {
        PlanInstance planInstance = PlanInstance.newBuilder().setPlanId(3L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                        .addChanges(getResizeScenarioChanges(StringConstants.DISABLED))
                        .addChanges(ScenarioChange.newBuilder()
                                .setRiSetting(RISetting.newBuilder().build())))).build();
        Assert.assertTrue(!PlanRpcServiceUtil.isScalingEnabled(planInstance.getScenario().getScenarioInfo()));
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

    /**
     * Verify that listeners receive updates for the READY -> QUEUED and QUEUED -> ANALYSIS
     * state transitions.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPlanQueueStatusChange() throws Exception {
        PlanServiceBlockingStub planRpcService = server.getBean(PlanServiceBlockingStub.class);
        final PlanListener listener = Mockito.mock(PlanListener.class);
        client.addPlanListener(listener);
        final long planId = planDao.createPlanInstance(CreatePlanRequest.getDefaultInstance()).getPlanId();

        planRpcService.runPlan(PlanId.newBuilder().setPlanId(planId).build());

        final StatusMatcher queuedMatcher = new StatusMatcher(PlanStatus.QUEUED);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT).atLeastOnce())
                .onPlanStatusChanged(queuedMatcher.capture());

        final StatusMatcher constructingTopologyMatcher =
                new StatusMatcher(PlanStatus.CONSTRUCTING_TOPOLOGY);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT).atLeastOnce())
                .onPlanStatusChanged(constructingTopologyMatcher.capture());
    }

    /**
     * Tests finishing a plan, when firstly the projected topology is reported, and later - the
     * action plan is reported.
     *
     * @throws Exception if exceptions occur.
     */
    @Test
    public void testFinishTopologyAndHistoryThenActions() throws Exception {
        final PlanListener listener = Mockito.mock(PlanListener.class);
        client.addPlanListener(listener);
        final long planId = planDao.createPlanInstance(CreatePlanRequest.getDefaultInstance()).getPlanId();
        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        actionsListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());
        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());
        final StatusMatcher inprogressMatcher = new StatusMatcher(PlanStatus.WAITING_FOR_RESULT);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onPlanStatusChanged(inprogressMatcher.capture());
        Assert.assertEquals(TOPOLOGY_ID, inprogressMatcher.getValue().getProjectedTopologyId());
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, inprogressMatcher.getValue().getStatus());

        actionsListener.onActionsUpdated(
                ActionsUpdated.newBuilder()
                        .setActionPlanId(ACT_PLAN_ID)
                        .setActionPlanInfo(ActionPlanInfo.newBuilder()
                                .setMarket(MarketActionPlanInfo.newBuilder()
                                        .setSourceTopologyInfo(TopologyInfo.newBuilder()
                                                .setTopologyContextId(planId)))).build());
        actionsListener
                .onStatsAvailable(StatsAvailable.newBuilder().setTopologyContextId(1).build());
        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        actionsListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
        actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                .build());
        actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                .build());

        final StatusMatcher matcher = new StatusMatcher(PlanStatus.SUCCEEDED);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT)).onPlanStatusChanged(matcher.capture());
        final PlanInstance plan = matcher.getValue();
        assertPlan(plan, planId);
    }

    @Nonnull
    private ActionsUpdated actionsUpdated(final long actionPlanId, final long planId) {
        return ActionsUpdated.newBuilder()
            .setActionPlanId(actionPlanId)
            .setActionPlanInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planId))))
            .build();
    }

    /**
     * Test finishing a plan when the projected topology and actions are received first,
     * and the history notification comes in later.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testFinishTopologyAndActionsThenHistory() throws Exception {
        final PlanListener listener = Mockito.mock(PlanListener.class);
        client.addPlanListener(listener);
        final long planId = planDao.createPlanInstance(CreatePlanRequest.getDefaultInstance()).getPlanId();

        actionsListener.onActionsUpdated(actionsUpdated(ACT_PLAN_ID, planId));
        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);

        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());

        final StatusMatcher inprogressMatcher = new StatusMatcher(PlanStatus.WAITING_FOR_RESULT);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onPlanStatusChanged(inprogressMatcher.capture());
        Assert.assertTrue(ACT_PLAN_ID == inprogressMatcher.getValue().getActionPlanIdList().get(0));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, inprogressMatcher.getValue().getStatus());

        actionsListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());
        actionsListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
        actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                .build());
        actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                .build());

        final StatusMatcher captor = new StatusMatcher(PlanStatus.SUCCEEDED);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT)).onPlanStatusChanged(captor.capture());
        final PlanInstance plan = captor.getValue();
        assertPlan(plan, planId);
    }

    /**
     * Tests finishing a plan, when firstly the action plan is reported, and later - the
     * projected topology is reported.
     *
     * @throws Exception if exceptions occur.
     */
    @Test
    public void testFinishActionsAndHistoryThenTopology() throws Exception {
        final PlanListener listener = Mockito.mock(PlanListener.class);
        client.addPlanListener(listener);
        final long planId = planDao.createPlanInstance(CreatePlanRequest.getDefaultInstance()).getPlanId();
        actionsListener.onActionsUpdated(actionsUpdated(ACT_PLAN_ID, planId));

        actionsListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());
        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());

        final StatusMatcher inprogressMatcher = new StatusMatcher(PlanStatus.WAITING_FOR_RESULT);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onPlanStatusChanged(inprogressMatcher.capture());
        Assert.assertTrue(ACT_PLAN_ID == inprogressMatcher.getValue().getActionPlanIdList().get(0));
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, inprogressMatcher.getValue().getStatus());

        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        final StatusMatcher captor = new StatusMatcher(PlanStatus.SUCCEEDED);
        actionsListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
        actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                .build());
        actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                .setStatusUpdate(StatusUpdate.newBuilder()
                        .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                        .setStatus(Status.SUCCESS)
                        .build())
                .build());

        Mockito.verify(listener, Mockito.timeout(TIMEOUT)).onPlanStatusChanged(captor.capture());
        final PlanInstance plan = captor.getValue();
        assertPlan(plan, planId);
    }

    /**
     * Tests when a topology is reported twice. It is expected, that plan is not reported as
     * finished.
     *
     * @throws Exception if exceptions occur.
     */
    @Test
    public void testDoubleTopologyReported() throws Exception {
        final PlanListener listener = Mockito.mock(PlanListener.class);
        client.addPlanListener(listener);
        final long planId = planDao.createPlanInstance(CreatePlanRequest.getDefaultInstance()).getPlanId();
        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());
        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());
    }

    /**
     * Tests concurrent reporting of action plan and projected topology. If something is wrong
     * with concurrency in this case, the test may fail from time to time.
     *
     * @throws Exception in exceptions occur
     */
    @Test
    public void testFinishTopologyAndActionsSimultaneously() throws Exception {
        final PlanListener listener = Mockito.mock(PlanListener.class);
        client.addPlanListener(listener);
        final long planId = planDao.createPlanInstance(CreatePlanRequest.getDefaultInstance()).getPlanId();
        final CountDownLatch latch = new CountDownLatch(1);
        final Future<?> actionReport = threadPool.submit(() -> {
            latch.await();
            actionsListener.onActionsUpdated(actionsUpdated(ACT_PLAN_ID, planId));
            return null;
        });
        final Future<?> topologyReport = threadPool.submit(() -> {
            latch.await();
            actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
            return null;
        });
        final Future<?> historyStatsAvailable = threadPool.submit(() -> {
            latch.await();
            actionsListener.onStatsAvailable(StatsAvailable.newBuilder()
                .setTopologyContextId(planId)
                .build());
            return null;
        });
        final Future<?> sourceTopologyAvailable = threadPool.submit(() -> {
            latch.await();
            actionsListener.onSourceTopologyAvailable(TOPOLOGY_ID, planId);
            return null;
        });
        final Future<?> costNotificationAvailable = threadPool.submit(() -> {
            latch.await();
            actionsListener.onCostNotificationReceived(CostNotification.newBuilder()
                    .setStatusUpdate(StatusUpdate.newBuilder()
                            .setType(StatusUpdateType.PROJECTED_COST_UPDATE)
                            .setStatus(Status.SUCCESS)
                            .build())
                    .build());
            return null;
        });
        latch.countDown();

        actionReport.get();
        topologyReport.get();
        historyStatsAvailable.get();

        final StatusMatcher captor = new StatusMatcher(PlanStatus.SUCCEEDED);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT)).onPlanStatusChanged(captor.capture());
        final PlanInstance plan = captor.getValue();
        assertPlan(plan, planId);
    }

    /**
     * Test that the {@link PlanProgressListener} ignores notifications about the real-time
     * topology.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testIgnoreRealtimeTopology() throws Exception {
        actionsListener.onActionsUpdated(actionsUpdated(1, PlanTestConfig.REALTIME_TOPOLOGY_ID));

        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
        actionsListener.onProjectedTopologyAvailable(0, PlanTestConfig.REALTIME_TOPOLOGY_ID);
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
        Mockito.verify(reservationPlacementHandler, Mockito.times(1))
                .updateReservations(Mockito.anyLong(), Mockito.anyLong());
        actionsListener.onProjectedTopologyFailure(0, PlanTestConfig.REALTIME_TOPOLOGY_ID, "");
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
    }

    private static void assertPlan(@Nonnull final PlanInstance plan, final long planId) {
        Assert.assertEquals(PlanStatus.SUCCEEDED, plan.getStatus());
        Assert.assertEquals(planId, plan.getPlanId());
        Assert.assertEquals(TOPOLOGY_ID, plan.getProjectedTopologyId());
        Assert.assertTrue(ACT_PLAN_ID == plan.getActionPlanIdList().get(0));
    }

    /**
     * Creates a matcher to match only succeeded plan instances.
     *
     * @return matcher representation for verification
     */
    private static PlanInstance planSucceeded() {
        return new StatusMatcher(PlanStatus.SUCCEEDED).capture();
    }

    /**
     * Matcher to capture {@link PlanInstance}s of the specific status.
     */
    private static class StatusMatcher extends BaseMatcher<PlanInstance> implements
            CapturesArguments {

        private final List<PlanInstance> plans = new ArrayList<>();
        private final PlanStatus status;

        public StatusMatcher(PlanStatus status) {
            this.status = status;
        }

        @Override
        public boolean matches(Object item) {
            final PlanInstance plan = (PlanInstance)item;
            return plan.getStatus() == status;
        }

        @Override
        public void describeTo(Description description) {
           description.appendText(status.toString());
        }

        public List<PlanInstance> getValues() {
            return plans;
        }

        public PlanInstance getValue() {
            Assert.assertEquals(1, plans.size());
            return plans.get(0);
        }

        @Override
        public void captureFrom(Object argument) {
            plans.add((PlanInstance) argument);
        }

        public PlanInstance capture() {
            return Mockito.argThat(this);
        }
    }
}
