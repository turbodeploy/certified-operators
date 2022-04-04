package com.vmturbo.action.orchestrator.rpc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator;
import com.vmturbo.action.orchestrator.action.ActionSchedule;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.ExecutedActionsChangeWindowDao;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionCombiner;
import com.vmturbo.action.orchestrator.execution.ActionExecutionStore;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionChain;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionExecution;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionChainsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ResendAuditedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.ResendAuditedActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * test helper methods {@link ActionsRpcService#getActionCountsByDateResponseBuilder}.
 */
public class ActionsRpcServiceTest {
    private static final long ACTION_PLAN_ID = 9876;
    private static final long ASSOCIATED_ID_ACCT = 123123;
    private static final long ASSOCIATED_RESOURCE_GROUP_ID = 111;
    private static final long CONTEXT_ID = 777777L;
    private static final long ACTION_LEGACY_INSTANCE_ID = 8822L;
    private static final long ACTION_STABLE_IMPACT_ID = 2244L;
    private static final long ACTION_ID_1 = 100L;
    private static final long RECOMMENDATION_ID_1 = 200L;
    private static final long RECOMMENDATION_ID_2 = 201L;
    private static final long ACTION_ID_3 = 102L;
    private static final long RECOMMENDATION_ID_3 = 202L;
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final ActionDTO.MultiActionRequest request = ActionDTO.MultiActionRequest.newBuilder()
        .addActionIds(ACTION_LEGACY_INSTANCE_ID)
        .setTopologyContextId(CONTEXT_ID)
        .build();

    private ActionStorehouse actionStorehouse;
    private ActionApprovalManager actionApprovalManager;
    private ActionTranslator actionTranslator;
    private ActionPaginator.ActionPaginatorFactory paginatorFactory;
    private HistoricalActionStatReader historicalActionStatReader;
    private CurrentActionStatReader currentActionStatReader;
    private UserSessionContext userSessionContext;
    private AcceptedActionsDAO acceptedActionsStore;
    private RejectedActionsDAO rejectedActionsStore;
    private ActionStore actionStore;
    private ActionsRpcService actionsRpcService;
    private ActionsRpcService actionsByImpactOidRpcService;
    private ActionAuditSender actionAuditSender;
    private AuditedActionsManager auditedActionsManager;
    private ActionExecutionStore actionExecutionStore;
    private ActionCombiner actionCombiner;
    private ActionAutomationManager actionAutomationManager;
    private ActionHistoryDao actionHistoryDao;
    private ExecutedActionsChangeWindowDao executedActionsChangeWindowDao;
    private final ActionTopologyStore actionTopologyStore = mock(ActionTopologyStore.class);

    private final SettingPolicyServiceMole settingPolicyServiceSpy = spy(new SettingPolicyServiceMole());
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(settingPolicyServiceSpy);

    /**
     * Setups the environment for test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        actionStorehouse = mock(ActionStorehouse.class);
        actionApprovalManager = mock(ActionApprovalManager.class);
        when(actionTopologyStore.getSourceTopology()).thenReturn(Optional.empty());
        actionTranslator = new ActionTranslator(server.getChannel(), actionTopologyStore);
        paginatorFactory = mock(ActionPaginator.ActionPaginatorFactory.class);
        historicalActionStatReader = mock(HistoricalActionStatReader.class);
        currentActionStatReader = mock(CurrentActionStatReader.class);
        userSessionContext = mock(UserSessionContext.class);
        acceptedActionsStore = mock(AcceptedActionsDAO.class);
        rejectedActionsStore = mock(RejectedActionsDAO.class);
        actionStore = mock(ActionStore.class);
        actionAuditSender = Mockito.mock(ActionAuditSender.class);
        auditedActionsManager = Mockito.mock(AuditedActionsManager.class);
        actionAutomationManager = Mockito.mock(ActionAutomationManager.class);
        actionExecutionStore = new ActionExecutionStore();
        actionCombiner = mock(ActionCombiner.class);
        actionHistoryDao = mock(ActionHistoryDao.class);
        executedActionsChangeWindowDao = mock(ExecutedActionsChangeWindowDao.class);

        actionsRpcService = new ActionsRpcService(
                Clock.systemUTC(),
                actionStorehouse,
                actionApprovalManager,
                actionTranslator,
                paginatorFactory,
                historicalActionStatReader,
                currentActionStatReader,
                userSessionContext,
                acceptedActionsStore,
                rejectedActionsStore,
                auditedActionsManager,
                actionAuditSender,
                actionExecutionStore,
                actionCombiner,
                actionAutomationManager,
                actionHistoryDao,
                executedActionsChangeWindowDao,
                10,
                777777L);
        actionsByImpactOidRpcService = new ActionsRpcService(
                null,
                actionStorehouse,
                actionApprovalManager,
                actionTranslator,
                paginatorFactory,
                historicalActionStatReader,
                currentActionStatReader,
                userSessionContext,
                acceptedActionsStore,
                rejectedActionsStore,
                auditedActionsManager,
                actionAuditSender,
                actionExecutionStore,
                actionCombiner,
                actionAutomationManager,
                actionHistoryDao,
                executedActionsChangeWindowDao,
                10,
                777777L);
        when(actionStorehouse.getStore(CONTEXT_ID)).thenReturn(Optional.of(actionStore));
    }

    @Test
    public void testGetActionCountsByDateResponseBuilder() throws Exception {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1/*srcType*/, 2L, 1/*desType*/, 10L/*tgtId*/, ActionState.SUCCEEDED);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 2/*srcType*/, 3L, 2/*destType*/, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 3/*srcType*/, 2L, 3/*destType*/, 12L, ActionState.FAILED);

        List<ActionView> actionViewList = ImmutableList.of(actionView1, actionView2, actionView3);
        final long k1 = 1111L;
        final long k2 = 2222L;
        final Map<Long, List<ActionView>> actionViewsMap = ImmutableMap.of(
                k1, actionViewList, k2, actionViewList);

        Builder builder = ActionsRpcService.getActionCountsByDateResponseBuilder(actionViewsMap);
        assertEquals(k1, builder.getActionCountsByDateBuilderList().get(0).getDate());
        assertEquals(k2, builder.getActionCountsByDateBuilderList().get(1).getDate());
        // one is mode = manual, state = succeeded, the second is mode = manual, state = failed
        assertEquals(2, builder.getActionCountsByDateBuilderList().get(0).getCountsByStateAndModeCount());
        assertEquals(actionView1.getDescription(),"Move VM10 from PM1 to PM2");
        assertEquals(actionView2.getDescription(),"Move VM11 from PM1 to PM3");
        assertEquals(actionView3.getDescription(),"Move VM12 from PM4 to PM2");
    }


    @Test
    public void testGetActionCountsByDateResponseBuilderWithTwoTypes() throws Exception {
        final ActionView actionView1 =
            executableMoveAction(123L, 1L, 1/*srcType*/, 2L, 1/*desType*/, 10L/*tgtId*/, ActionState.SUCCEEDED);
        final ActionView actionView2 =
            executableMoveAction(124L, 1L, 2/*srcType*/, 3L, 2/*destType*/, 11L, ActionState.SUCCEEDED);
        final ActionView actionView3 =
            executableMoveAction(125L, 4L, 3/*srcType*/, 2L, 3/*destType*/, 12L, ActionState.FAILED);

        final ActionView actionView4 = executableActivateAction(126L, ACTION_STABLE_IMPACT_ID, 13L);
        final ActionView actionView5 = executableActivateAction(127L, ACTION_STABLE_IMPACT_ID, 14L);

        List<ActionView> actionViewList1 = ImmutableList.of(
                actionView1, actionView2, actionView3);
        List<ActionView> actionViewList2 = ImmutableList.of(
                actionView4, actionView5);
        final long k1 = 1111L;
        final long k2 = 2222L;
        final Map<Long, List<ActionView>> actionViewsMap = ImmutableMap.of(
                k1, actionViewList1, k2, actionViewList2);

        Builder builder = ActionsRpcService.getActionCountsByDateResponseBuilder(actionViewsMap);
        assertEquals(2, builder.getActionCountsByDateBuilderList().size());
        assertEquals(k1, builder.getActionCountsByDateBuilderList().get(0).getDate());
        assertEquals(k2, builder.getActionCountsByDateBuilderList().get(1).getDate());
        assertEquals(actionView1.getDescription(),"Move VM10 from PM1 to PM2");
        assertEquals(actionView2.getDescription(),"Move VM11 from PM1 to PM3");
        assertEquals(actionView3.getDescription(),"Move VM12 from PM4 to PM2");
    }

    /**
     * Test acceptance of action.
     *
     * @throws ExecutionInitiationException never.
     */
    @Test
    public void testAcceptAction() throws ExecutionInitiationException {
        // ARRANGE
        Action action = executableActivateAction(ACTION_LEGACY_INSTANCE_ID, ACTION_STABLE_IMPACT_ID, 13L);
        when(actionStore.getAction(ACTION_LEGACY_INSTANCE_ID)).thenReturn(Optional.of(action));

        doThrow(new ExecutionInitiationException("test", Status.Code.INTERNAL))
                .when(actionApprovalManager).attemptAcceptAndExecute(eq(actionStore), any(), eq(action));
        StreamObserver<ActionDTO.ActionExecution> observer = mock(StreamObserver.class);

        // ACT
        actionsRpcService.acceptActions(request, observer);

        // ASSERT
        verifyEmptyResult(observer);
    }

    /**
     * Tests acceptance of action when the action is expired.
     *
     * @throws ExecutionInitiationException never.
     */
    @Test
    public void testAcceptedActionExpiredSchedule() throws ExecutionInitiationException {
        Action action = executableActivateAction(ACTION_LEGACY_INSTANCE_ID, ACTION_STABLE_IMPACT_ID, 13L);
        ActionSchedule actionSchedule = new ActionSchedule(null, null, "America/Toronto", 11L,
            "Schedule 1", ActionDTO.ActionMode.MANUAL, null);
        action.setSchedule(actionSchedule);
        when(actionStore.getAction(ACTION_LEGACY_INSTANCE_ID)).thenReturn(Optional.of(action));

        StreamObserver<ActionDTO.ActionExecution> observer = mock(StreamObserver.class);

        // ACT
        actionsRpcService.acceptActions(request, observer);

        // ASSERT
        verifyEmptyResult(observer);
    }

    /**
     * Tests that {@link ActionsRpcService#resendAuditEvents(ResendAuditedActionsRequest, StreamObserver)}
     * triggers resending only of actual actions from audited bookkeeping cache.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testResendAuditedActions() throws Exception {
        // ARRANGE
        final long actualActionStableId = 1L;
        final long clearedActionStableId = 2L;
        final long auditWorkflowId = 3L;
        final long targetEntityId = 4L;
        final long clearedTargetEntityId = 5L;
        final String settingName = ActionSettingSpecs.getSubSettingFromActionModeSetting(
            ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
            ActionSettingType.ON_GEN);
        final AuditedActionInfo auditedAction =
                new AuditedActionInfo(actualActionStableId, auditWorkflowId, targetEntityId, settingName, Optional.empty());
        final AuditedActionInfo auditedAndClearedAction =
                new AuditedActionInfo(clearedActionStableId, auditWorkflowId, clearedTargetEntityId, settingName,
                        Optional.of(System.currentTimeMillis()));
        Mockito.when(auditedActionsManager.getAlreadySentActions(auditWorkflowId))
                .thenReturn(Arrays.asList(auditedAction, auditedAndClearedAction));
        final StreamObserver<ResendAuditedActionsResponse> observer =
                Mockito.mock(StreamObserver.class);
        final ResendAuditedActionsRequest resendActionsRequest =
                ResendAuditedActionsRequest.newBuilder().setWorkflowId(auditWorkflowId).build();
        final ActionView actualActionMock = new Action(
                ActionOrchestratorTestUtils.createMoveRecommendation(actualActionStableId), 19,
                actionModeCalculator, actualActionStableId);
        final QueryableActionViews actionViews = Mockito.mock(QueryableActionViews.class);
        Mockito.when(actionStore.getActionViews()).thenReturn(actionViews);
        Mockito.when(actionViews.getByRecommendationId(Collections.singleton(actualActionStableId)))
                .thenReturn(Stream.of(actualActionMock));
        Mockito.when(actionAuditSender.resendActionEvents(
                Mockito.eq(Collections.singleton(actualActionMock)))).thenReturn(1);

        // ACT
        actionsRpcService.resendAuditEvents(resendActionsRequest, observer);

        // ASSERT
        Mockito.verify(observer)
                .onNext(Mockito.eq(ResendAuditedActionsResponse.newBuilder()
                        .setAuditedActionsCount(1)
                        .build()));
        Mockito.verify(observer).onCompleted();
        Mockito.verify(actionAuditSender)
                .resendActionEvents(Collections.singleton(actualActionMock));
    }

    /**
     * Test case when resend request has missed workflow id.
     */
    @Test
    public void testFailedResendAuditedActions() {
        // ARRANGE
        final StreamObserver<ResendAuditedActionsResponse> observer =
                Mockito.mock(StreamObserver.class);
        final ResendAuditedActionsRequest resendActionsRequestWithMissedWorkflow =
                ResendAuditedActionsRequest.newBuilder().build();
        final ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

        // ACT
        actionsRpcService.resendAuditEvents(resendActionsRequestWithMissedWorkflow, observer);

        // ASSERT
        Mockito.verify(observer).onError(exceptionCaptor.capture());
        Assert.assertThat(exceptionCaptor.getValue().getMessage(),
                CoreMatchers.containsString("Missing required parameter 'workflowId"));
        Mockito.verify(observer, Mockito.never()).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onCompleted();
    }

    /**
     * Test getting the instance id of actions based on the recommendation id.
     */
    @Test
    public void testGetInstanceIdsForRecommendationIds() {
        // ARRANGE
        final Action action1 = mock(Action.class);
        when(action1.getRecommendationOid()).thenReturn(RECOMMENDATION_ID_1);
        when(action1.getId()).thenReturn(ACTION_ID_1);
        when(actionStore.getActionByRecommendationId(RECOMMENDATION_ID_1))
                .thenReturn(Optional.of(action1));
        when(actionStore.getActionByRecommendationId(RECOMMENDATION_ID_2))
                .thenReturn(Optional.empty());
        final Action action3 = mock(Action.class);
        when(action3.getRecommendationOid()).thenReturn(RECOMMENDATION_ID_3);
        when(action3.getId()).thenReturn(ACTION_ID_3);
        when(actionStore.getActionByRecommendationId(RECOMMENDATION_ID_3))
                .thenReturn(Optional.of(action3));


        final StreamObserver<GetInstanceIdsForRecommendationIdsResponse> observer =
                Mockito.mock(StreamObserver.class);
        ArgumentCaptor<GetInstanceIdsForRecommendationIdsResponse> argumentCaptor =
                ArgumentCaptor.forClass(GetInstanceIdsForRecommendationIdsResponse.class);


        // ACT
        actionsRpcService.getInstanceIdsForRecommendationIds(GetInstanceIdsForRecommendationIdsRequest
                .newBuilder()
                .setTopologyContextId(CONTEXT_ID)
                .addRecommendationId(RECOMMENDATION_ID_1)
                .addRecommendationId(RECOMMENDATION_ID_2)
                .addRecommendationId(RECOMMENDATION_ID_3)
                .build(), observer);

        // ASSERT
        verify(observer).onNext(argumentCaptor.capture());
        verify(observer, Mockito.never()).onError(Mockito.any());
        verify(observer).onCompleted();

        Map<Long, Long> resultMap = argumentCaptor.getValue().getRecommendationIdToInstanceIdMap();
        assertThat(resultMap.size(), equalTo(2));
        assertThat(resultMap.get(RECOMMENDATION_ID_1), equalTo(ACTION_ID_1));
        assertThat(resultMap.get(RECOMMENDATION_ID_3), equalTo(ACTION_ID_3));
    }

    /**
     * Test getting the instance id of actions when topology context is not set.
     */
    @Test
    public void testGetInstanceIdsForRecommendationIdsTopologyNotSet() {
        // ARRANGE
        final StreamObserver<GetInstanceIdsForRecommendationIdsResponse> observer =
                Mockito.mock(StreamObserver.class);
        final ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

        // ACT
        actionsRpcService.getInstanceIdsForRecommendationIds(GetInstanceIdsForRecommendationIdsRequest
                .newBuilder()
                .addRecommendationId(RECOMMENDATION_ID_1)
                .build(), observer);

        //ASSERT
        Mockito.verify(observer).onError(exceptionCaptor.capture());
        Assert.assertThat(exceptionCaptor.getValue().getMessage(),
                CoreMatchers.containsString("Missing required parameter topologyContextId."));
        Mockito.verify(observer, Mockito.never()).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onCompleted();
    }

    /**
     * Tests getting the instance id of actions when store for the context does not exist
     */
    @Test
    public void testGetInstanceIdsForRecommendationIdsStoreDoesNotExist() {
        // ARRANGE
        final StreamObserver<GetInstanceIdsForRecommendationIdsResponse> observer =
                Mockito.mock(StreamObserver.class);
        final ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
        when(actionStorehouse.getStore(11L)).thenReturn(Optional.empty());

        // ACT
        actionsRpcService.getInstanceIdsForRecommendationIds(GetInstanceIdsForRecommendationIdsRequest
                .newBuilder()
                .setTopologyContextId(11L)
                .addRecommendationId(RECOMMENDATION_ID_1)
                .build(), observer);

        //ASSERT
        Mockito.verify(observer).onError(exceptionCaptor.capture());
        Assert.assertThat(exceptionCaptor.getValue().getMessage(),
                CoreMatchers.containsString("Action store for context 11 not found"));
        Mockito.verify(observer, Mockito.never()).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onCompleted();
    }

    private Action executableMoveAction(
                long id,
                long sourceId,
                int sourceType,
                long destId,
                int destType,
                long targetId,
                ActionState state) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
            .setId(id)
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(TestActionBuilder
                .makeMoveInfo(targetId, sourceId, sourceType, destId, destType))
            .build();

        String actionDescription = "Move VM"+targetId+" from PM"+sourceId+" to PM"+destId;
        SerializationState orchestratorAction = new SerializationState(ACTION_PLAN_ID,
            action,
            LocalDateTime.now(),
            ActionDecision.getDefaultInstance(),
            ExecutionStep.getDefaultInstance(),
            state,
            new ActionTranslation(action),
            ASSOCIATED_ID_ACCT,
            ASSOCIATED_RESOURCE_GROUP_ID,
            actionDescription.getBytes(),
                2244L);
        return spy(new Action(orchestratorAction, actionModeCalculator));
    }

    private Action executableActivateAction(long legacyInstanceId, long stableImpactId, long targetId) {
        final ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(legacyInstanceId)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder()
                                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                                .build())
                        .build())
                .build();

        return spy(new Action(action, ACTION_PLAN_ID, actionModeCalculator, stableImpactId));
    }

    private static void verifyEmptyResult(StreamObserver<ActionDTO.ActionExecution> observer) {
        ArgumentCaptor<ActionExecution> actionExecutionArg = ArgumentCaptor.forClass(
                ActionExecution.class);
        verify(observer).onNext(actionExecutionArg.capture());
        assertEquals(0, actionExecutionArg.getValue().getActionIdCount());
        verify(observer).onCompleted();
    }

    /**
     * Test the logic in getActionChains method.
     */
    @Test
    public void testGetActionChains() {
        long entityId1 = 1000;
        long entityId2 = 1001;
        GetActionChainsRequest request = GetActionChainsRequest.newBuilder()
                .addEntityOid(entityId1)
                .addEntityOid(entityId2)
                .build();

        Map<Long, List<ExecutedActionsChangeWindow>> recordsByEntity = new HashMap<>();
        LocalDateTime firstActionTimeEntity1 = LocalDateTime.of(2022, 3, 10, 9, 15);
        recordsByEntity.put(entityId1, createExecutedActionsChangeWindowList(entityId1, Arrays.asList(200L, 201L),
                firstActionTimeEntity1));
        LocalDateTime firstActionTimeEntity2 = LocalDateTime.of(2022, 3, 15, 10, 30);
        recordsByEntity.put(entityId2, createExecutedActionsChangeWindowList(entityId2, Arrays.asList(300L, 301L, 302L),
                firstActionTimeEntity2));
        when(executedActionsChangeWindowDao.getExecutedActionsChangeWindowMap(request.getEntityOidList()))
                .thenReturn(recordsByEntity);

        List<ActionView> actionViewList = new ArrayList<>();
        actionViewList.add(createAction(200L, entityId1, firstActionTimeEntity1));
        actionViewList.add(createAction(201L, entityId1, firstActionTimeEntity1.plusDays(1)));
        actionViewList.add(createAction(300L, entityId2, firstActionTimeEntity2));
        actionViewList.add(createAction(301L, entityId2, firstActionTimeEntity2.plusDays(1)));
        actionViewList.add(createAction(302L, entityId2, firstActionTimeEntity2.plusDays(2)));

        ArgumentCaptor<List> actionIdCaptor = ArgumentCaptor.forClass(List.class);
        when(actionHistoryDao.getActionHistoryByIds(actionIdCaptor.capture())).thenReturn(actionViewList);

        final StreamObserver<ActionChain> observer = Mockito.mock(StreamObserver.class);

        actionsRpcService.getActionChains(request, observer);

        // Verify the list of action Ids passed to the actionHistoryDao::getActionHistoryByIds
        // contains all action IDs.
        List<Long> capturedActionIds = (List<Long>)actionIdCaptor.getValue();
        List<Long> expectedActionList = Arrays.asList(200L, 201L, 300L, 301L, 302L);
        assertTrue(expectedActionList.containsAll(capturedActionIds));

        // Verify onNext is invoked twice because there are two entities. There is one action chain
        // per entity.
        ArgumentCaptor<ActionChain> actionChainCaptor = ArgumentCaptor.forClass(ActionChain.class);
        verify(observer, times(2)).onNext(actionChainCaptor.capture());

        // Verify action chain of the first entity. The chain should have 2 ExecutedActionsChangeWindow
        // records.
        List<ActionChain> actionChains = actionChainCaptor.getAllValues();
        assertEquals(entityId1, actionChains.get(0).getEntityOid());
        List<ActionDTO.ExecutedActionsChangeWindow> windows = actionChains.get(0).getExecutedActionsChangeWindowList();
        assertEquals(2, windows.size());

        // Verify the action IDs, entity IDs and start time in the ExecutedActionsChangeWindow records.
        assertTrue(Arrays.asList(200L, 201L).containsAll(windows.stream()
                .map(x -> x.getActionSpec().getRecommendation().getId()).collect(Collectors.toList())));
        assertTrue(Arrays.asList(entityId1, entityId1, entityId1).containsAll(windows.stream()
                .map(ActionDTO.ExecutedActionsChangeWindow::getEntityOid).collect(Collectors.toList())));
        assertTrue(Arrays.asList(convertToLong(firstActionTimeEntity1),
                convertToLong(firstActionTimeEntity1.plusDays(1))).containsAll(windows.stream()
                .map(ActionDTO.ExecutedActionsChangeWindow::getStartTime).collect(Collectors.toList())));

        // Verify the action chain of the second entity. The chain should have 3 ExecutedActionsChangeWindow
        // records.
        assertEquals(entityId2, actionChains.get(1).getEntityOid());
        windows = actionChains.get(1).getExecutedActionsChangeWindowList();
        assertEquals(3, windows.size());

        // Verify the action IDs, entity IDs and start time in the ExecutedActionsChangeWindow records.
        assertTrue(Arrays.asList(300L, 301L, 302L).containsAll(windows.stream()
                .map(x -> x.getActionSpec().getRecommendation().getId()).collect(Collectors.toList())));
        assertTrue(Arrays.asList(entityId2, entityId2, entityId2).containsAll(windows.stream()
                .map(ActionDTO.ExecutedActionsChangeWindow::getEntityOid).collect(Collectors.toList())));
        assertTrue(Arrays.asList(convertToLong(firstActionTimeEntity2),
                convertToLong(firstActionTimeEntity2.plusDays(1)),
                convertToLong(firstActionTimeEntity2.plusDays(2))).containsAll(windows.stream()
                .map(ActionDTO.ExecutedActionsChangeWindow::getStartTime).collect(Collectors.toList())));

        // Make sure the onCompleted method is called.
        verify(observer).onCompleted();
    }

    private long convertToLong(LocalDateTime dateTime) {
        return TimeUtil.localTimeToMillis(dateTime, Clock.systemUTC());
    }

    private ActionView createAction(Long actionId, Long entityId, LocalDateTime actionCompletionTime) {
        ActionDTO.Action recommendation = ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .setDeprecatedImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                        .setScale(Scale.newBuilder()
                                .setTarget(ActionEntity.newBuilder()
                                        .setId(entityId)
                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                        .build())
                                .build()))
                .build();

        return new Action(new SerializationState(
                actionId,
                recommendation,
                LocalDateTime.of(2022, 3, 10, 10, 30),
                null,
                ExecutionStep.newBuilder()
                        .setStatus(ExecutionStep.Status.SUCCESS)
                        .setCompletionTime(TimeUtil.localTimeToMillis(actionCompletionTime, Clock.systemUTC()))
                        .build(),
                ActionState.SUCCEEDED,
                new ActionTranslation(recommendation),
                null,
                null,
                null,
                null),
                actionModeCalculator);
    }

    private List<ExecutedActionsChangeWindow> createExecutedActionsChangeWindowList(
            long entityId, List<Long> actionIds, final LocalDateTime dateTime) {
        List<ExecutedActionsChangeWindow> list = new ArrayList<>();
        LocalDateTime actionDate = dateTime.plusDays(0);
        for (Long actionId : actionIds) {
            list.add(ExecutedActionsChangeWindow.newBuilder()
                    .setEntityOid(entityId)
                    .setActionOid(actionId)
                    .setStartTime(convertToLong(actionDate)).build());
            actionDate = actionDate.plusDays(1);
        }
        return list;
    }
}
