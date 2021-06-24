package com.vmturbo.action.orchestrator.rpc;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.passthroughTranslator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.ImmutableActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.state.machine.Transition.TransitionResult;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreLoader;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.topology.processor.api.ActionExecutionListener;

/**
 * Tests for action execution RPCs.
 */
public class ActionExecutionRpcTest {

    private static final Optional<WorkflowDTO.Workflow> EMPTY_WORKFLOW_OPTIONAL = Optional.empty();

    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    // Have the translator pass-through translate all actions.
    private final ActionTranslator actionTranslator = passthroughTranslator();
    final AtomicActionSpecsCache atomicActionSpecsCache = Mockito.spy(new AtomicActionSpecsCache());
    final AtomicActionFactory atomicActionFactory = Mockito.spy(new AtomicActionFactory(atomicActionSpecsCache));

    private ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    private final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);
    private final RejectedActionsDAO rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);
    private final IActionStoreFactory actionStoreFactory = mock(IActionStoreFactory.class);
    private final IActionStoreLoader actionStoreLoader = mock(IActionStoreLoader.class);
    private final ActionHistoryDao actionHistoryDao = mock(ActionHistoryDao.class);
    private final ActionExecutionListener actionExecutionListener =
        Mockito.mock(ActionExecutionListener.class);
    private final ActionAuditSender actionAuditSender = mock(ActionAuditSender.class);
    private final AuditedActionsManager auditedActionsManager = mock(AuditedActionsManager.class);

    private final ActionExecutor actionExecutor = mock(ActionExecutor.class);
    private final ProbeCapabilityCache probeCapabilityCache = mock(ProbeCapabilityCache.class);
    private final ActionTargetSelector actionTargetSelector = mock(ActionTargetSelector.class);
    private final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
            actionStoreLoader, mock(ActionAutomationManager.class), false);
    private final ActionPaginatorFactory paginatorFactory = mock(ActionPaginatorFactory.class);

    private final HistoricalActionStatReader statReader = mock(HistoricalActionStatReader.class);

    private final CurrentActionStatReader liveStatReader = mock(CurrentActionStatReader.class);

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache = mock(EntitiesAndSettingsSnapshotFactory.class);

    private final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

    private final LiveActionsStatistician statistician = mock(LiveActionsStatistician.class);

    private final UserSessionContext userSessionContext = mock(UserSessionContext.class);
    private IdentityServiceImpl actionIdentityService;

    private final LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);

    private final InvolvedEntitiesExpander involvedEntitiesExpander =
        mock(InvolvedEntitiesExpander.class);

    private static final long ACTION_PLAN_ID = 2;
    private static final long TOPOLOGY_CONTEXT_ID = 3;
    private static final long ACTION_ID = 9999;

    private final Clock clock = new MutableFixedClock(1_000_000);

    private ActionsRpcService actionsRpcService;
    private ActionApprovalManager actionApprovalManager;

    private final SettingPolicyServiceMole settingPolicyServiceMole = new SettingPolicyServiceMole();
    private final SupplyChainServiceMole supplyChainServiceMole = spy(new SupplyChainServiceMole());
    private final RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());
    private final EntitySeverityCache entitySeverityCache = mock(EntitySeverityCache.class);
    private final WorkflowStore workflowStore = mock(WorkflowStore.class);

    private GrpcTestServer grpcServer;
    private ActionStore actionStoreSpy;

    private LiveActionPipelineFactory pipelineFactory;

    /**
     * Sets up the tests.
     *
     * @throws Exception on exception occurred
     */
    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        actionApprovalManager = new ActionApprovalManager(actionExecutor, actionTargetSelector,
                entitySettingsCache, actionTranslator, Mockito.mock(WorkflowStore.class),
                acceptedActionsStore, actionExecutionListener);
        actionIdentityService = Mockito.mock(IdentityServiceImpl.class);
        Mockito.when(actionIdentityService.getOidsForObjects(Mockito.any()))
                .thenReturn(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L));
        actionsRpcService = new ActionsRpcService(
            clock,
            actionStorehouse,
            actionApprovalManager,
            actionTranslator,
            paginatorFactory,
            statReader,
            liveStatReader,
            userSessionContext,
            acceptedActionsStore,
            rejectedActionsStore,
            auditedActionsManager,
            actionAuditSender,
            500,
            false,
            777777L);
        grpcServer = GrpcTestServer.newServer(actionsRpcService, settingPolicyServiceMole,
                supplyChainServiceMole, repositoryServiceMole);
        grpcServer.start();
        ActionTargetInfo targetInfo = ImmutableActionTargetInfo.builder()
            .supportingLevel(SupportLevel.SUPPORTED)
            .targetId(123)
            .build();
        when(actionTargetSelector.getTargetsForActions(any(), any(), any())).thenAnswer(invocation -> {
            Stream<ActionDTO.Action> actions = invocation.getArgumentAt(0, Stream.class);
            return actions.collect(Collectors.toMap(ActionDTO.Action::getId, action -> targetInfo));
        });
        when(actionTargetSelector.getTargetForAction(any(), any(), any())).thenReturn(targetInfo);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(licenseCheckClient.hasValidNonExpiredLicense()).thenReturn(true);
        actionStoreSpy = Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                actionTargetSelector, entitySettingsCache, actionHistoryDao,
            actionTranslator, clock, userSessionContext,
                licenseCheckClient, acceptedActionsStore, rejectedActionsStore,
                actionIdentityService, involvedEntitiesExpander,
            entitySeverityCache, workflowStore));

        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);
        when(actionStoreLoader.loadActionStores()).thenReturn(Collections.emptyList());
        when(actionStoreFactory.getContextTypeName(anyLong())).thenReturn("foo");

        pipelineFactory = new LiveActionPipelineFactory(actionStorehouse, mock(ActionAutomationManager.class),
            atomicActionFactory, entitySettingsCache, 10, probeCapabilityCache,
            actionHistoryDao, actionFactory, clock, 10,
            actionIdentityService, actionTargetSelector, actionTranslator, statistician,
            actionAuditSender);
    }

    /**
     * Cleans up resources.
     */
    @After
    public void cleanup() {
        grpcServer.close();
    }

    /**
     * Test accepting an existing action.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptAction() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        AcceptActionResponse response =  actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        assertTrue(response.hasActionSpec());
        assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        assertEquals(ActionState.IN_PROGRESS, response.getActionSpec().getActionState());
    }

    /**
     * Tests persisting acceptance for action with execution schedule window.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPersistingAcceptanceForActionWithExecutionSchedule() throws Exception {
        final long actionTargetId = 4L;
        final long executionScheduleId = 111L;
        final ActionDTO.Action recommendation =
                ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID,
                        actionTargetId, 1, 1, 2, 1);
        final ScheduleProto.Schedule executionSchedule =
                ActionOrchestratorTestUtils.createActiveSchedule(executionScheduleId);

        final ActionPlan plan = actionPlan(recommendation);
        final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(
                snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getSettingsForEntity(eq(actionTargetId))).thenReturn(
                ActionOrchestratorTestUtils.makeActionModeAndExecutionScheduleSetting(
                        ActionMode.MANUAL, Collections.singleton(executionScheduleId)));
        when(snapshot.getScheduleMap()).thenReturn(
                ImmutableMap.of(executionScheduleId, executionSchedule));
        when(snapshot.getSettingPoliciesForEntity(actionTargetId)).thenReturn(
                new ImmutableMap.Builder<String, Collection<Long>>().put(
                        ConfigurableActionSettings.Move.getSettingName(), Collections.singletonList(22L))
                        .put(ActionSettingSpecs.getSubSettingFromActionModeSetting(
                            ConfigurableActionSettings.Move, ActionSettingType.SCHEDULE),
                            Collections.singleton(23L))
                        .build());

        pipelineFactory.actionPipeline(plan).run(plan);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();
        final AcceptActionResponse response =
                actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        Assert.assertTrue(response.hasActionSpec());
        Assert.assertEquals(ACTION_ID, response.getActionSpec().getRecommendation().getId());
        final Action action = actionStoreSpy.getAction(ACTION_ID).get();
        Mockito.verify(acceptedActionsStore)
                .persistAcceptedAction(Mockito.eq(action.getRecommendationOid()),
                        Mockito.any(LocalDateTime.class), Mockito.any(String.class),
                        Mockito.any(LocalDateTime.class), Mockito.any(String.class),
                        Mockito.anyCollectionOf(Long.class));
        // check that for accepted actions updated accepting user
        Assert.assertEquals("SYSTEM", action.getSchedule().get().getAcceptingUser());
    }

    /**
     * Failed to persist acceptance for action if there are no associated policies.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testFailedPersistingAcceptance() throws Exception {
        final long actionTargetId = 4L;
        final long executionScheduleId = 111L;
        final ActionDTO.Action recommendation =
                ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID, actionTargetId, 1,
                        1, 2, 1);
        final ScheduleProto.Schedule executionSchedule =
                ActionOrchestratorTestUtils.createActiveSchedule(executionScheduleId);

        final ActionPlan plan = actionPlan(recommendation);
        final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(
                snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getSettingsForEntity(eq(actionTargetId))).thenReturn(
                ActionOrchestratorTestUtils.makeActionModeAndExecutionScheduleSetting(
                        ActionMode.MANUAL, Collections.singleton(executionScheduleId)));
        when(snapshot.getScheduleMap()).thenReturn(
                ImmutableMap.of(executionScheduleId, executionSchedule));

        pipelineFactory.actionPipeline(plan).run(plan);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
                .setActionId(ACTION_ID)
                .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
                .build();

        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionRequest);
        } catch (StatusRuntimeException ex) {
            Assert.assertThat(ex.getMessage(), CoreMatchers.containsString(
                "Failed to persist acceptance for action " + ACTION_ID));
            return;
        }
        fail("The call should throw an exception");

    }

    /**
     * Test accepting an action that's not found.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testAcceptActionNotFound() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get().overwriteActions(ImmutableMap.of(
            ActionPlanType.MARKET, Collections.emptyList())); // Clear the action from the store

        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionRequest);
        } catch (StatusRuntimeException ex) {
            assertEquals("NOT_FOUND: Action " + ACTION_ID + " doesn't exist.", ex.getMessage());
            return;
        }
        fail("The call should throw an exception");
    }

    @Test
    public void testAcceptTopologyContextNotFound() throws Exception {
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionRequest);
        } catch (StatusRuntimeException ex) {
            assertEquals("NOT_FOUND: Unknown topology context: " + TOPOLOGY_CONTEXT_ID,
                ex.getMessage());
            return;
        }
        fail("The call should throw an exception");
    }

    @Test
    public void testAcceptActionUpdatesSeverityCache() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);

        EntitySeverityCache severityCacheMock = mock(EntitySeverityCache.class);
        doReturn(Optional.of(severityCacheMock)).when(actionStoreSpy).getEntitySeverityCache();

        pipelineFactory.actionPipeline(plan).run(plan);
        actionOrchestratorServiceClient.acceptAction(acceptActionRequest);

        Mockito.verify(severityCacheMock).refresh(
            eq(plan.getAction(0)),
            eq(actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get())
        );
    }

    @Test
    public void testFailedToPassAcceptanceGuard() throws Exception {
        ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionRequest = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);

        // Replace the action in the store with a spy that can be mocked
        ActionStore actionStore = actionStorehouse.getStore(TOPOLOGY_CONTEXT_ID).get();
        Action actionSpy = Mockito.spy(actionStore.getAction(ACTION_ID).get());
        Mockito.doReturn(new TransitionResult<>(ActionState.READY, ActionState.READY, false))
            .when(actionSpy).receive(Mockito.any(AcceptanceEvent.class));
        actionStore.overwriteActions(ImmutableMap.of(ActionPlanType.MARKET, Collections.singletonList(actionSpy)));

        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionRequest);
        } catch (StatusRuntimeException ex) {
            assertEquals(ex.getStatus().getCode(), Code.PERMISSION_DENIED);
            assertThat(ex.getStatus().getDescription(), CoreMatchers.containsString(
                    "Action cannot be executed, because transition"
                            + " was blocked by acceptance guard. Action mode:"
                            + actionSpy.getMode()));
            return;
        }
        fail("The call should throw an exception");
    }

    @Test
    public void testTargetResolutionError() throws Exception {
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);
        when(actionTargetSelector.getTargetForAction(eq(recommendation), any(), any()))
            .thenReturn(ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.UNSUPPORTED)
                .build());

        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionContext);
        } catch (StatusRuntimeException ex) {
            Assert.assertThat(ex.getMessage(), CoreMatchers.containsString("Action cannot be executed by "
                + "any target"));
            return;
        }
        fail("The call should throw an exception");
    }

    @Test
    public void testExecutionError() throws Exception {
        final long targetId = 7777;
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();
        EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        when(snapshot.getOwnerAccountOfEntity(anyLong())).thenReturn(Optional.empty());
        when(snapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());

        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot,recommendation);

        pipelineFactory.actionPipeline(plan).run(plan);

        when(actionTargetSelector.getTargetForAction(Mockito.eq(recommendation), any(), any())).thenReturn(
            ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.SUPPORTED)
                .targetId(targetId)
                .build());
        doThrow(new ExecutionStartException("ERROR!"))
            .when(actionExecutor).execute(eq(targetId), any(), eq(EMPTY_WORKFLOW_OPTIONAL));


        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionContext);
        } catch (StatusRuntimeException ex) {
            assertThat(ex.getMessage(), CoreMatchers.containsString("ERROR!"));
            return;
        }
        fail("The call should throw an exception");
    }

    /**
     * Test that execution will be failed for accepted action if it don't have successful
     * translation.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testTranslationError() throws Exception {
        final long targetId = 7777;
        final ActionDTO.Action recommendation = ActionOrchestratorTestUtils.createMoveRecommendation(ACTION_ID);
        final ActionPlan plan = actionPlan(recommendation);
        final SingleActionRequest acceptActionContext = SingleActionRequest.newBuilder()
            .setActionId(ACTION_ID)
            .setTopologyContextId(TOPOLOGY_CONTEXT_ID)
            .build();

        when(entitySettingsCache.newSnapshot(any(), any(), anyLong(), anyLong())).thenReturn(snapshot);
        ActionOrchestratorTestUtils.setEntityAndSourceAndDestination(snapshot, recommendation);

        final AtomicActionSpecsCache atomicActionSpecsCache = Mockito.spy(new AtomicActionSpecsCache());
        final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
        final ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory,
                actionStoreLoader, Mockito.mock(ActionAutomationManager.class), false);
        // We use actionTranslator which can successfully translate action in order to calculate
        // appropriate action mode (>= MANUAL). As a result action can be available for acceptance
        // and possible execution.
        final ActionsRpcService actionsRpcService =
                new ActionsRpcService(
                    clock,
                    actionStorehouse,
                    actionApprovalManager,
                    actionTranslator,
                    paginatorFactory,
                    statReader,
                    liveStatReader,
                    userSessionContext,
                    acceptedActionsStore,
                    rejectedActionsStore,
                    auditedActionsManager,
                    actionAuditSender,
                    500,
                    false,
                    777777L);
        final GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsRpcService,
                supplyChainServiceMole, repositoryServiceMole);
        grpcServer.start();
        final ActionsServiceBlockingStub actionOrchestratorServiceClient =
                ActionsServiceGrpc.newBlockingStub(
            grpcServer.getChannel());
        final IActionFactory actionFactory = new ActionFactory(actionModeCalculator);
        actionStoreSpy = Mockito.spy(new LiveActionStore(actionFactory, TOPOLOGY_CONTEXT_ID,
                actionTargetSelector, entitySettingsCache, actionHistoryDao,
            actionTranslator, clock, userSessionContext,
                licenseCheckClient, acceptedActionsStore, rejectedActionsStore,
                actionIdentityService, involvedEntitiesExpander,
            entitySeverityCache, workflowStore));
        when(actionStoreFactory.newStore(anyLong())).thenReturn(actionStoreSpy);

        pipelineFactory = new LiveActionPipelineFactory(actionStorehouse, mock(ActionAutomationManager.class),
            atomicActionFactory, entitySettingsCache, 10, probeCapabilityCache,
            actionHistoryDao, actionFactory, clock, 10,
            actionIdentityService, actionTargetSelector, actionTranslator, statistician,
            actionAuditSender);

        pipelineFactory.actionPipeline(plan).run(plan);
        when(actionTargetSelector.getTargetForAction(Mockito.eq(recommendation), any(), any())).thenReturn(
            ImmutableActionTargetInfo.builder()
                .supportingLevel(SupportLevel.SUPPORTED)
                .targetId(targetId)
                .build());

        // emulate failed translation for action
        final Action action = actionStoreSpy.getAction(ACTION_ID).get();
        action.getActionTranslation().setTranslationFailure();

        try {
            actionOrchestratorServiceClient.acceptAction(acceptActionContext);
        } catch (StatusRuntimeException ex) {
            final String errorMessage = "Failed to translate action " + ACTION_ID + " for execution.";
            assertThat(ex.getMessage(), CoreMatchers.containsString(errorMessage));
            assertEquals(action.getState(), ActionState.FAILED);
            grpcServer.close();
            return;
        }
        fail("The call should throw an exception");
    }

    private static ActionPlan actionPlan(ActionDTO.Action recommendation) {
        return ActionPlan.newBuilder()
            .setId(ACTION_PLAN_ID)
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(TOPOLOGY_CONTEXT_ID))))
            .addAction(recommendation)
            .build();
    }
}
