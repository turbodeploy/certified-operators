package com.vmturbo.action.orchestrator.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorNotificationReceiver;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.test.IntegrationTestServer;

/**
 * Integration test for Action Orchestrator API client and server.
 */
public class ActionOrchestratorApiIntegrationTest {

    private static final int TIMEOUT_MS = 30000;

    private static final Logger logger = LogManager.getLogger();

    private IntegrationTestServer integrationTestServer;

    private ActionOrchestratorNotificationSender notificationSender;

    private ActionOrchestratorNotificationReceiver actionOrchestrator;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestName testName = new TestName();

    @Captor
    private ArgumentCaptor<ActionsUpdated> actionsUpdatedCaptor;

    @Captor
    private ArgumentCaptor<ActionProgress> progressCaptor;

    @Captor
    private ArgumentCaptor<ActionSuccess> successCaptor;

    @Captor
    private ArgumentCaptor<ActionFailure> failureCaptor;

    private IMessageReceiver<ActionOrchestratorNotification> messageReceiver;

    @Before
    public final void init() throws Exception {

        MockitoAnnotations.initMocks(this);

        Thread.currentThread().setName(testName.getMethodName() + "-main");
        logger.debug("Starting @Before");
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("clt-" + testName.getMethodName() + "-%d").build();
        final ExecutorService threadPool = Executors.newCachedThreadPool(threadFactory);

        integrationTestServer = new IntegrationTestServer(testName, TestApiServerConfig.class);
        messageReceiver = integrationTestServer.getBean("notificationsChannel");
        actionOrchestrator =
                new ActionOrchestratorNotificationReceiver(messageReceiver, threadPool, 0);

        notificationSender =
                integrationTestServer.getBean(ActionOrchestratorNotificationSender.class);
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
        actionOrchestrator.addListener(listener);

        final ActionDTO.ActionPlanInfo actionPlanInfo = ActionPlanInfo.newBuilder()
            .setMarket(MarketActionPlanInfo.newBuilder()
                .setSourceTopologyInfo(TopologyInfo.newBuilder()
                    .setTopologyId(1)))
            .build();
        final ActionDTO.ActionPlan actionPlan = ActionDTO.ActionPlan.newBuilder()
            .setId(0L)
            .setInfo(actionPlanInfo)
            .addAction(ActionOrchestratorTestUtils.createMoveRecommendation(1L))
            .build();
        notificationSender.notifyActionsUpdated(actionPlan);

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1)).onActionsUpdated(actionsUpdatedCaptor.capture());

        final ActionsUpdated actionsUpdated = actionsUpdatedCaptor.getValue();
        Assert.assertEquals(actionPlan.getId(), actionsUpdated.getActionPlanId());
        Assert.assertEquals(actionPlan.getInfo(), actionsUpdated.getActionPlanInfo());
    }

    @Test
    public void testNotifyProgress() throws Exception {
        final ActionsListener listener = Mockito.mock(ActionsListener.class);
        actionOrchestrator.addListener(listener);

        ActionProgress actionProgress = ActionProgress.newBuilder()
            .setDescription("progress")
            .setActionId(1234)
            .setProgressPercentage(33)
            .build();
        notificationSender.notifyActionProgress(actionProgress);

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1)).onActionProgress(progressCaptor.capture());

        final ActionProgress progress = progressCaptor.getValue();
        Assert.assertEquals(33, progress.getProgressPercentage());
        Assert.assertEquals("progress", progress.getDescription());
        Assert.assertEquals(1234, progress.getActionId());
    }

    @Test
    public void testNotifySuccess() throws Exception {
        final ActionsListener listener = Mockito.mock(ActionsListener.class);
        actionOrchestrator.addListener(listener);

        ActionSuccess actionSuccess = ActionSuccess.newBuilder()
            .setSuccessDescription("success")
            .setActionId(2345)
            .build();
        notificationSender.notifyActionSuccess(actionSuccess);

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1)).onActionSuccess(successCaptor.capture());

        final ActionSuccess success = successCaptor.getValue();
        Assert.assertEquals("success", success.getSuccessDescription());
        Assert.assertEquals(2345, success.getActionId());
    }

    @Test
    public void testNotifyFailure() throws Exception {
        final ActionsListener listener = Mockito.mock(ActionsListener.class);
        actionOrchestrator.addListener(listener);

        ActionFailure actionFailure = ActionFailure.newBuilder()
            .setErrorDescription("error")
            .setActionId(2345)
            .build();
        notificationSender.notifyActionFailure(actionFailure);

        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS).times(1)).onActionFailure(failureCaptor.capture());

        final ActionFailure failure = failureCaptor.getValue();
        Assert.assertEquals("error", failure.getErrorDescription());
        Assert.assertEquals(2345, failure.getActionId());
    }
}