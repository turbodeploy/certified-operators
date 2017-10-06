package com.vmturbo.plan.orchestrator.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.plan.orchestrator.api.PlanListener;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;

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

    private PlanProgressListener actionsListener;

    private final Logger logger = LogManager.getLogger();

    @Before
    public void init() throws Exception {
        server = new IntegrationTestServer(testName, PlanTestConfigWebsocket.class);
        threadPool = Executors.newCachedThreadPool();
        client = new PlanOrchestratorClientImpl(server.connectionConfig(), threadPool);
        planDao = server.getBean(PlanDao.class);
        actionsListener = server.getBean(PlanProgressListener.class);
    }

    @After
    public void shutdown() throws Exception {
        client.close();
        server.close();
        threadPool.shutdownNow();
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
        server.waitForRegisteredEndpoints(1, TIMEOUT);

        planRpcService.runPlan(PlanId.newBuilder().setPlanId(planId).build());

        final StatusMatcher queuedMatcher = new StatusMatcher(PlanStatus.QUEUED);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT).atLeastOnce())
                .onPlanStatusChanged(queuedMatcher.capture());

        final StatusMatcher constructingTopologyMatcher =
                new StatusMatcher(PlanStatus.CONSTRUCTING_TOPOLOGY);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT).atLeastOnce())
                .onPlanStatusChanged(constructingTopologyMatcher.capture());

        final StatusMatcher analysisMatcher = new StatusMatcher(PlanStatus.RUNNING_ANALYSIS);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT).atLeastOnce())
                .onPlanStatusChanged(analysisMatcher.capture());
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
        server.waitForRegisteredEndpoints(1, TIMEOUT);
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

        actionsListener.onActionsReceived(
                ActionPlan.newBuilder().setId(ACT_PLAN_ID).setTopologyContextId(planId).build());
        final StatusMatcher matcher = new StatusMatcher(PlanStatus.SUCCEEDED);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT)).onPlanStatusChanged(matcher.capture());
        final PlanInstance plan = matcher.getValue();
        assertPlan(plan, planId);
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
        server.waitForRegisteredEndpoints(1, TIMEOUT);

        actionsListener.onActionsReceived(
                ActionPlan.newBuilder().setId(ACT_PLAN_ID).setTopologyContextId(planId).build());
        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);

        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());

        final StatusMatcher inprogressMatcher = new StatusMatcher(PlanStatus.WAITING_FOR_RESULT);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onPlanStatusChanged(inprogressMatcher.capture());
        Assert.assertEquals(ACT_PLAN_ID, inprogressMatcher.getValue().getActionPlanId());
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, inprogressMatcher.getValue().getStatus());

        actionsListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
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
        server.waitForRegisteredEndpoints(1, TIMEOUT);

        actionsListener.onActionsReceived(
                ActionPlan.newBuilder().setId(ACT_PLAN_ID).setTopologyContextId(planId).build());

        actionsListener.onStatsAvailable(StatsAvailable.newBuilder()
            .setTopologyContextId(planId)
            .build());
        Mockito.verify(listener, Mockito.never()).onPlanStatusChanged(planSucceeded());

        final StatusMatcher inprogressMatcher = new StatusMatcher(PlanStatus.WAITING_FOR_RESULT);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT))
                .onPlanStatusChanged(inprogressMatcher.capture());
        Assert.assertEquals(ACT_PLAN_ID, inprogressMatcher.getValue().getActionPlanId());
        Assert.assertEquals(PlanStatus.WAITING_FOR_RESULT, inprogressMatcher.getValue().getStatus());

        actionsListener.onProjectedTopologyAvailable(TOPOLOGY_ID, planId);
        final StatusMatcher captor = new StatusMatcher(PlanStatus.SUCCEEDED);
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
        server.waitForRegisteredEndpoints(1, TIMEOUT);
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
        server.waitForRegisteredEndpoints(1, TIMEOUT);
        final CountDownLatch latch = new CountDownLatch(1);
        final Future<?> actionReport = threadPool.submit(() -> {
            final ActionPlan actionPlan =
                    ActionPlan.newBuilder().setId(ACT_PLAN_ID).setTopologyContextId(planId).build();
            latch.await();
            actionsListener.onActionsReceived(actionPlan);
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
        actionsListener.onActionsReceived(ActionPlan.newBuilder()
            .setId(1L)
            .setTopologyContextId(PlanTestConfig.REALTIME_TOPOLOGY_ID)
            .build());
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());

        actionsListener.onProjectedTopologyAvailable(0, PlanTestConfig.REALTIME_TOPOLOGY_ID);
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());

        actionsListener.onProjectedTopologyFailure(0, PlanTestConfig.REALTIME_TOPOLOGY_ID, "");
        Mockito.verify(planDao, Mockito.never()).updatePlanInstance(Mockito.anyLong(), Mockito.any());
    }

    private static void assertPlan(@Nonnull final PlanInstance plan, final long planId) {
        Assert.assertEquals(PlanStatus.SUCCEEDED, plan.getStatus());
        Assert.assertEquals(planId, plan.getPlanId());
        Assert.assertEquals(TOPOLOGY_ID, plan.getProjectedTopologyId());
        Assert.assertEquals(ACT_PLAN_ID, plan.getActionPlanId());
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
