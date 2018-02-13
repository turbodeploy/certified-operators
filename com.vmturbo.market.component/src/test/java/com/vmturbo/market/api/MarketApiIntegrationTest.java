package com.vmturbo.market.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;

/**
 * Integration test for Market API client and server.
 */
public class MarketApiIntegrationTest {

    private static final int TIMEOUT_MS = 30000;

    private static final Logger logger = LogManager.getLogger();

    private IntegrationTestServer integrationTestServer;

    private ExecutorService threadPool;

    private MarketNotificationSender notificationSender;

    protected MarketComponentNotificationReceiver market;

    @Rule
    public TestName testName = new TestName();

    @Captor
    private ArgumentCaptor<ActionPlan> actionCaptor;

    @Before
    public final void init() throws Exception {

        MockitoAnnotations.initMocks(this);

        Thread.currentThread().setName(testName.getMethodName() + "-main");
        logger.debug("Starting @Before");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("clt-" + testName.getMethodName() + "-%d").build();
        threadPool = Executors.newCachedThreadPool(threadFactory);

        integrationTestServer = new IntegrationTestServer(testName, TestApiServerConfig.class);
        market = new MarketComponentNotificationReceiver(
                integrationTestServer.getBean("projectedTopologySender"),
                integrationTestServer.getBean("actionPlanSender"),
                integrationTestServer.getBean("priceIndexSender"),
                integrationTestServer.getBean("planAnalysisTopologySender"),
                threadPool);

        notificationSender = integrationTestServer.getBean(MarketNotificationSender.class);

        logger.debug("Finished @Before");
    }

    @After
    public final void shutdown() throws Exception {
        logger.debug("Starting @After");
        integrationTestServer.close();
        logger.debug("Finished @After");
    }

    /**
     * Test that action notifications on the server-side
     * propagate to clients, and clients return acks.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testNotifyActions() throws Exception {
        final ActionsListener listener = Mockito.mock(ActionsListener.class);
        market.addActionsListener(listener);

        ActionPlan actionPlan = ActionPlan.newBuilder()
                .setId(0L)
                .setTopologyId(0L)
                .addAction(createAction())
                .build();
        notificationSender.notifyActionsRecommended(actionPlan);

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1)).onActionsReceived(actionCaptor.capture());

        final ActionPlan receivedActions = actionCaptor.getValue();
        Assert.assertEquals(actionPlan.getActionCount(), receivedActions.getActionCount());
        Assert.assertEquals(actionPlan.getAction(0), receivedActions.getAction(0));
    }

    private Action createAction() {
        return Action.newBuilder()
                .setId(0L)
                .setImportance(0)
                .setExplanation(ActionDTO.Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                            .setTargetId(0L)
                            .addChanges(ChangeProvider.newBuilder()
                                .setSourceId(0L)
                                .setDestinationId(0L)
                                .build())
                            .build())
                        .build())
                .build();
    }

}
