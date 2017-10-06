package com.vmturbo.components.test.performance.action.orchestrator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Channel;
import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClient;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.communication.MarketStub;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;

@Alert({"ao_populate_store_duration_seconds_sum{store_type='Live'}/5minutes",
        "ao_populate_store_duration_seconds_sum{store_type='Plan'}/5minutes",
        "ao_delete_plan_action_plan_duration_seconds_sum",
        "jvm_memory_bytes_used_max"})
public class ActionOrchestratorPerformanceTest {

    private static final Logger logger = LogManager.getLogger();

    private MarketStub marketStub = new MarketStub();

    private Channel actionOrchestratorChannel;

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
            .withComponentCluster(ComponentCluster.newBuilder()
                .withService(ComponentCluster.newService("action-orchestrator")
                        .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                        .withMemLimit(2.5, MetricPrefix.GIGA)
                        .logsToLogger(logger)))
        .withStubs(ComponentStubHost.newBuilder()
            .withNotificationStubs(marketStub))
        .scrapeClusterAndLocalMetricsToInflux();

    @BeforeClass
    public static void setupClass() {
        IdentityGenerator.initPrefix(0);
    }

    private ActionOrchestrator actionOrchestrator;
    private ActionsServiceBlockingStub actionsService;
    private ExecutorService threadPool = Executors.newCachedThreadPool();

    @Before
    public void setup() {
        actionOrchestratorChannel = componentTestRule.getCluster().newGrpcChannel("action-orchestrator");

        actionOrchestrator = ActionOrchestratorClient.rpcAndNotification(
            componentTestRule.getCluster().getConnectionConfig("action-orchestrator"),
            threadPool);
        actionsService = ActionsServiceGrpc.newBlockingStub(actionOrchestratorChannel);
    }

    @After
    public void teardown() {
        try {
            actionOrchestrator.close();

            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in ActionOrchestratorPerformanceTest!", e);
        }
    }

    @Test
    public void test100kActionPlan() throws Exception {
        testPlanActionPlan(100_000);
        testLiveActionPlan(100_000);
    }

    @Test
    public void test200kActionPlan() throws Exception  {
        testPlanActionPlan(200_000);
        testLiveActionPlan(200_000);
    }

    @Test
    public void test300kActionPlan() throws Exception  {
        testPlanActionPlan(300_000);
        testLiveActionPlan(300_000);
    }

    @Test
    public void test400kActionPlan() throws Exception  {
        testPlanActionPlan(400_000);
        testLiveActionPlan(400_000);
    }

    /**
     * 1. Send a live action plan.
     * 2. Fetch the live action plan.
     */
    public void testLiveActionPlan(int actionPlanSize) throws Exception {
        final ActionPlanGenerator actionPlanGenerator = new ActionPlanGenerator();
        final ActionPlan sendActionPlan = actionPlanGenerator.generate(actionPlanSize, 1,
            ComponentUtils.REALTIME_TOPOLOGY_CONTEXT);
        populateActions(actionOrchestrator, sendActionPlan, "LIVE");

        fetchActions(actionsService, FilteredActionRequest.newBuilder().setFilter(
                ActionQueryFilter.newBuilder()
                    .setVisible(true)
            ).setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                .build(), "VISIBLE LIVE");

        fetchActions(actionsService, FilteredActionRequest.newBuilder()
                .setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                .build(), "ALL LIVE");
    }

    /**
     * 1. Send a plan action plan.
     * 2. Fetch the plan action plan.
     * 3. Delete the plan action plan.
     */
    public void testPlanActionPlan(int actionPlanSize) throws Exception {
        final long planContextId = 0xABC;

        final ActionPlanGenerator actionPlanGenerator = new ActionPlanGenerator();
        final ActionPlan sendActionPlan = actionPlanGenerator.generate(actionPlanSize, 1, planContextId);
        populateActions(actionOrchestrator, sendActionPlan, "PLAN");

        fetchActions(actionsService, FilteredActionRequest.newBuilder()
            .setTopologyContextId(planContextId)
            .build(), "PLAN");

        final long start = System.currentTimeMillis();
        final DeleteActionsResponse deleteActionsResponse = actionsService.deleteActions(
            DeleteActionsRequest.newBuilder()
                .setTopologyContextId(planContextId)
                .build());

        logger.info("Took {} seconds to delete {} PLAN actions.",
            (System.currentTimeMillis() - start) / 1000.0f,
            deleteActionsResponse.getActionCount());
    }

    public void populateActions(@Nonnull final ActionOrchestrator actionOrchestrator,
                                @Nonnull final ActionPlan actionPlan,
                                @Nonnull final String type) throws Exception {
        final CompletableFuture<ActionPlan> actionPlanFuture = new CompletableFuture<>();
        actionOrchestrator.addActionsListener(new TestActionsListener(actionPlanFuture));

        final long start = System.currentTimeMillis();
        marketStub.getBackend().notifyActionsRecommended(actionPlan);
        final ActionPlan receivedActionPlan = actionPlanFuture.get(10, TimeUnit.MINUTES);

        logger.info("Took {} seconds to receive and process {} action plan of size {}.",
            (System.currentTimeMillis() - start) / 1000.0f,
            type,
            receivedActionPlan.getActionList().size());
    }

    public void fetchActions(@Nonnull final ActionsServiceBlockingStub actionsService,
                             @Nonnull final FilteredActionRequest request,
                             @Nonnull final String type) throws Exception {
        final long startFetchVisible = System.currentTimeMillis();
        Iterable<ActionOrchestratorAction> fetchedActions = () -> actionsService.getAllActions(request);

        // The fetch is lazy - force evaluation and count them.
        final AtomicInteger counter = new AtomicInteger(0);
        StreamSupport.stream(fetchedActions.spliterator(), false)
            .forEach(action -> counter.getAndIncrement());

        logger.info("Took {} retrieve {} {} actions.",
            (System.currentTimeMillis() - startFetchVisible) / 1000.0f,
            type,
            counter.get());
    }

    private static class TestActionsListener implements ActionsListener {
        private final CompletableFuture<ActionPlan> actionPlanFuture;

        public TestActionsListener(@Nonnull final CompletableFuture<ActionPlan> actionPlanFuture) {
            this.actionPlanFuture = actionPlanFuture;
        }

        @Override
        public void onActionsReceived(@Nonnull final ActionPlan actionPlan) {
            actionPlanFuture.complete(actionPlan);
        }
    }
}
