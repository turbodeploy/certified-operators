package com.vmturbo.action.orchestrator.rpc;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.createActionEntity;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import io.grpc.Status.Code;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionPaginator;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.DefaultActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.PaginatedActionViews;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.PlanActionStore;
import com.vmturbo.action.orchestrator.store.query.MapBackedActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.TypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.ActionCountsByDateEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse.ActionCountsByEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.HistoricalActionStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.StateAndModeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextInfoRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionQueryRpcTest {

    private ActionsServiceBlockingStub actionOrchestratorServiceClient;

    private ActionsServiceBlockingStub actionOrchestratorServiceClientForFailedTranslation;

    private final ActionStorehouse actionStorehouse = Mockito.mock(ActionStorehouse.class);
    private final ActionStore actionStore = Mockito.mock(ActionStore.class);
    private final HistoricalActionStatReader historicalStatReader = mock(HistoricalActionStatReader.class);
    private final CurrentActionStatReader liveStatReader = mock(CurrentActionStatReader.class);
    private final ActionTranslator actionTranslator = ActionOrchestratorTestUtils.passthroughTranslator();
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    private final ActionTranslator actionTranslatorWithFailedTranslation =
        ActionOrchestratorTestUtils.passthroughTranslator();

    private final ActionPaginatorFactory paginatorFactory =
            Mockito.spy(new DefaultActionPaginatorFactory(1000, 1000));

    private final long actionPlanId = 2;
    private final long topologyContextId = 3;

    private final UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);

    private final Clock clock = new MutableFixedClock(1_000_000);
    private final AcceptedActionsDAO acceptedActionsStore = Mockito.mock(AcceptedActionsDAO.class);
    private final RejectedActionsDAO rejectedActionsStore = Mockito.mock(RejectedActionsDAO.class);


    private ActionsRpcService actionsRpcService;
    private ActionsRpcService actionsRpcServiceWithFailedTranslator;
    private ActionApprovalManager approvalManager;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private GrpcTestServer grpcServer;
    private GrpcTestServer grpcServerForFailedTranslation;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        approvalManager = Mockito.mock(ActionApprovalManager.class);
        actionsRpcService =
                new ActionsRpcService(clock, actionStorehouse, approvalManager, actionTranslator,
                        paginatorFactory, historicalStatReader, liveStatReader, userSessionContext,
                        acceptedActionsStore, rejectedActionsStore, 500);
        grpcServer = GrpcTestServer.newServer(actionsRpcService);
        grpcServer.start();

        actionsRpcServiceWithFailedTranslator =
                new ActionsRpcService(clock, actionStorehouse, approvalManager,
                        actionTranslatorWithFailedTranslation, paginatorFactory,
                        historicalStatReader, liveStatReader, userSessionContext,
                        acceptedActionsStore, rejectedActionsStore, 500);
        IdentityGenerator.initPrefix(0);
        actionOrchestratorServiceClient = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        grpcServerForFailedTranslation = GrpcTestServer.newServer(actionsRpcServiceWithFailedTranslator);
        grpcServerForFailedTranslation.start();
        actionOrchestratorServiceClientForFailedTranslation =
                ActionsServiceGrpc.newBlockingStub(grpcServerForFailedTranslation.getChannel());

        when(actionStorehouse.getStore(topologyContextId)).thenReturn(Optional.of(actionStore));
    }

    /**
     * Cleans up the tests.
     */
    @After
    public void cleanup() {
        grpcServer.close();
        grpcServerForFailedTranslation.close();
    }

    /**
     * Test a simple GET for an individual action.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetAction() throws Exception {
        final Action actionView = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        actionView.setDescription("Move VM1 from Host1 to Host2");
        when(actionStore.getActionView(1)).thenReturn(Optional.of(actionView));

        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setActionId(1)
            .build();

        ActionOrchestratorAction action = actionOrchestratorServiceClient.getAction(actionRequest);
        assertEquals(1, action.getActionId());
        assertTrue(action.hasActionSpec());
        assertEquals(spec(actionView), action.getActionSpec());
    }

    @Test
    public void testGetActionMissingArgument() throws Exception {
        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Missing required parameter actionId or topologyContextId."));

        actionOrchestratorServiceClient.getAction(actionRequest);
    }

    @Test
    public void testGetActionContextNotFound() throws Exception {
        when(actionStorehouse.getStore(1234)).thenReturn(Optional.empty());
        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .setTopologyContextId(1234)
            .setActionId(1)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("1234 not found"));
        actionOrchestratorServiceClient.getAction(actionRequest);
    }

    /**
     * Test a simple GET for a missing action.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetActionNotFound() throws Exception {
        when(actionStore.getActionView(1)).thenReturn(Optional.empty());

        SingleActionRequest actionRequest = SingleActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setActionId(1)
            .build();

        ActionOrchestratorAction action = actionOrchestratorServiceClient.getAction(actionRequest);
        assertEquals(1, action.getActionId());
        assertFalse(action.hasActionSpec());
    }

    @Test
    public void testGetAllActions() throws Exception {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final Action disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        visibleAction.setDescription("Move VM1 from Host1 to Host2");
        disabledAction.setDescription("Move VM2 from HostA to HostB");
        doReturn(ActionMode.DISABLED).when(disabledAction).getMode();
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction), PlanActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .build();

        final List<ActionSpec> actionSpecs = fetchSpecList(
                actionOrchestratorServiceClient.getAllActions(actionRequest));
        assertThat(actionSpecs, containsInAnyOrder(spec(visibleAction), spec(disabledAction)));
    }

    /**
     * Verify, we are NOT filtering out VCPU resize actions with different "from" and "to" values.
     * @throws Exception
     */
    @Test
    public void testGetAllActionWithResizeActionWithTranslation() throws Exception {
        final long id = 11l;
        Action resizeAction = new Action(ActionOrchestratorTestUtils
                .createResizeRecommendation(1, id, CommodityType.VCPU, 5000,
                        2500), actionPlanId, actionModeCalculator, 123L);
        resizeAction.setDescription("Resize down vcpu for VM1 from 5 to 2.5");
        final Resize newResize = ActionDTO.Resize.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.VCPU.getNumber()))
                .setOldCapacity((float)5)
                .setNewCapacity((float)2.5)
                .setTarget(createActionEntity(id)).build();
        resizeAction.getActionTranslation().setTranslationSuccess(
                resizeAction.getRecommendation().toBuilder().setInfo(
                        ActionInfo.newBuilder(resizeAction.getRecommendation().getInfo())
                                .setResize(newResize).build())
                        .build());
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
                resizeAction.getId(), resizeAction), PlanActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .build();

        final List<ActionSpec> actionSpecs = fetchSpecList(
                actionOrchestratorServiceClient.getAllActions(actionRequest));
        assertEquals(1, actionSpecs.size());
    }

    @Test
    public void testGetAllActionsPaginationParamsSetWithCursor() {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        visibleAction.setDescription("Move VM1 from HostA to HostB");
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
                visibleAction.getId(), visibleAction), PlanActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setLimit(10)
                .build();

        final PaginatedActionViews paginatedViews = Mockito.mock(PaginatedActionViews.class);
        when(paginatedViews.getResults()).thenReturn(Collections.singletonList(visibleAction));
        final String cursor = "THECURSOR";
        when(paginatedViews.getNextCursor()).thenReturn(Optional.of(cursor));

        final ActionPaginator mockPaginator = Mockito.mock(ActionPaginator.class);
        when(mockPaginator.applyPagination(any(), eq(paginationParameters))).thenReturn(paginatedViews);

        when(paginatorFactory.newPaginator()).thenReturn(mockPaginator);

        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setPaginationParams(paginationParameters)
                .build();

        final FilteredActionResponse response = actionOrchestratorServiceClient.getAllActions(actionRequest).next();
        assertTrue(response.hasPaginationResponse());
        assertThat(response.getPaginationResponse().getNextCursor(), is(cursor));
    }

    @Test
    public void testGetAllActionsPaginationParamsSetNoCursor() {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        visibleAction.setDescription("Move VM1 from Host1 to Host2");
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
                visibleAction.getId(), visibleAction), PlanActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setLimit(10)
                .build();

        final PaginatedActionViews paginatedViews = Mockito.mock(PaginatedActionViews.class);
        when(paginatedViews.getResults()).thenReturn(Collections.singletonList(visibleAction));
        when(paginatedViews.getNextCursor()).thenReturn(Optional.empty());

        final ActionPaginator mockPaginator = Mockito.mock(ActionPaginator.class);
        when(mockPaginator.applyPagination(any(), eq(paginationParameters))).thenReturn(paginatedViews);

        when(paginatorFactory.newPaginator()).thenReturn(mockPaginator);

        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setPaginationParams(paginationParameters)
                .build();

        final FilteredActionResponse response = actionOrchestratorServiceClient.getAllActions(actionRequest).next();
        assertTrue(response.hasPaginationResponse());
        assertFalse(response.getPaginationResponse().hasNextCursor());
    }

    @Test
    public void testGetFilteredActionsForRealTime() throws Exception {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final Action disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        visibleAction.setDescription("Move VM1 from Host1 to Host2");
        disabledAction.setDescription("Move VM2 from Host1 to Host2");
        doReturn(ActionMode.DISABLED).when(disabledAction).getMode();
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction), LiveActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);

        final FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setFilter(ActionQueryFilter.newBuilder().setVisible(true))
            .build();

        Iterators.advance(actionOrchestratorServiceClient.getAllActions(actionRequest), 1);

        final List<ActionSpec> resultSpecs = fetchSpecList(
                actionOrchestratorServiceClient.getAllActions(actionRequest));
        assertThat(resultSpecs, contains(spec(visibleAction)));
        assertThat(resultSpecs, not(contains(spec(disabledAction))));
    }

    // All plan actions should be visible.
    @Test
    public void testGetFilteredActionsForPlan() throws Exception {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final Action disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        visibleAction.setDescription("Move VM1 from Host1 to Host2");
        disabledAction.setDescription("Move VM2 from Host1 to Host2");
        doReturn(ActionMode.DISABLED).when(disabledAction).getMode();
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction), PlanActionStore.VISIBILITY_PREDICATE);
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(PlanActionStore.VISIBILITY_PREDICATE);

        final FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setFilter(ActionQueryFilter.newBuilder().setVisible(true))
            .build();

        final List<ActionSpec> actionSpecs = fetchSpecList(
                actionOrchestratorServiceClient.getAllActions(actionRequest));
        assertThat(actionSpecs, containsInAnyOrder(spec(visibleAction), spec(disabledAction)));
    }

    @Test
    public void testGetAllActionsMissingArgument() throws Exception {
        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("topologyContextId"));

        // Because getAllActions returns a lazily-evaluated stream, we have to force the evaluation somehow
        // to get the desired exception.
        actionOrchestratorServiceClient.getAllActions(actionRequest).next();
    }

    @Test
    public void testGetAllActionsContextNotFound() throws Exception {
        when(actionStorehouse.getStore(1234)).thenReturn(Optional.empty());
        FilteredActionRequest actionRequest = FilteredActionRequest.newBuilder()
            .setTopologyContextId(1234)
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("1234 not found"));

        // Because getAllActions returns a lazily-evaluated stream, we have to force the evaluation somehow
        // to get the desired exception.
        actionOrchestratorServiceClient.getAllActions(actionRequest).next();
    }

    @Test
    public void testGetMultiAction() throws Exception {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final Action disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        final Action notRetrievedAction = ActionOrchestratorTestUtils.createMoveAction(3, actionPlanId);
        visibleAction.setDescription("Move VM1 from Host1 to Host2");
        disabledAction.setDescription("Move VM2 from Host1 to Host2");
        notRetrievedAction.setDescription("Move VM3 from Host1 to Host2");

        doReturn(ActionMode.DISABLED).when(disabledAction).getMode();
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction,
            notRetrievedAction.getId(), notRetrievedAction));
        when(actionStore.getActionViews()).thenReturn(actionViews);

        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addActionIds(1)
            .addActionIds(2)
            .build();

        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getActions(actionRequest));

        assertThat(actionSpecs, containsInAnyOrder(spec(visibleAction), spec(disabledAction)));
        assertThat(actionSpecs, not(contains(spec(notRetrievedAction))));
    }

    /**
     * Test a GET for multiple actions, some of which are missing.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetMultiActionSomeMissing() throws Exception {
        final Action visibleAction = ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId);
        final Action disabledAction = spy(ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId));
        visibleAction.setDescription("Move VM1 from Host1 to Host2");
        disabledAction.setDescription("Move VM2 from Host1 to Host2");
        doReturn(ActionMode.DISABLED).when(disabledAction).getMode();
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            visibleAction.getId(), visibleAction,
            disabledAction.getId(), disabledAction));
        when(actionStore.getActionViews()).thenReturn(actionViews);

        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addActionIds(1)
            .addActionIds(2)
            .addActionIds(3)
            .build();

        Iterable<ActionOrchestratorAction> actionsIter = () -> actionOrchestratorServiceClient.getActions(actionRequest);
        Map<Long, Optional<ActionSpec>> actionSpecs = StreamSupport.stream(actionsIter.spliterator(), false)
            .collect(Collectors.toMap(
                ActionOrchestratorAction::getActionId,
                action -> action.hasActionSpec() ? Optional.of(action.getActionSpec()) : Optional.empty()
            ));

        assertEquals(Optional.of(spec(visibleAction)), actionSpecs.get(1L));
        assertEquals(Optional.of(spec(disabledAction)), actionSpecs.get(2L));
        assertEquals(Optional.empty(), actionSpecs.get(3L));
    }

    @Test
    public void testGetMultiActionMissingParameter() throws Exception {
        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .build();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("topologyContextId"));

        fetchSpecList(() -> actionOrchestratorServiceClient.getActions(actionRequest));
    }

    @Test
    public void testGetMultiActionUnknownTopologyContext() throws Exception {
        when(actionStorehouse.getStore(topologyContextId)).thenReturn(Optional.empty());

        MultiActionRequest actionRequest = MultiActionRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .addActionIds(1)
            .build();


        List<ActionSpec> actionSpecs = fetchSpecList(() -> actionOrchestratorServiceClient.getActions(actionRequest));
        assertTrue(actionSpecs.isEmpty());
    }

    @Test
    public void testGetAllActionContextsEmpty() throws Exception {
        when(actionStorehouse.getAllStores()).thenReturn(Collections.emptyMap());

        Iterable<TopologyContextResponse> responses = () -> actionOrchestratorServiceClient
            .getTopologyContextInfo(TopologyContextInfoRequest.getDefaultInstance());
        assertEquals(0, StreamSupport.stream(responses.spliterator(), false).count());
    }

    @Test
    public void testGetAllActionContextsSome() throws Exception {
        ActionStore store1 = Mockito.mock(ActionStore.class);
        when(store1.size()).thenReturn(123);
        ActionStore store2 = Mockito.mock(ActionStore.class);
        when(store2.size()).thenReturn(456);
        when(actionStorehouse.getAllStores()).thenReturn(ImmutableMap.of(
            123L, store1,
            456L, store2
        ));

        Iterable<TopologyContextResponse> responses = () -> actionOrchestratorServiceClient
            .getTopologyContextInfo(TopologyContextInfoRequest.getDefaultInstance());
        List<TopologyContextResponse> contexts = StreamSupport.stream(responses.spliterator(), false)
            .collect(Collectors.toList());

        assertEquals(2, contexts.size());
        assertThat(
            contexts.stream()
                .map(TopologyContextResponse::getTopologyContextId)
                .collect(Collectors.toList()),
            containsInAnyOrder(123L, 456L)
        );
        assertThat(
            contexts.stream()
                .map(TopologyContextResponse::getActionCount)
                .collect(Collectors.toList()),
            containsInAnyOrder(123, 456)
        );
    }

    @Test
    public void testGetAllActionCounts() throws Exception {
        final int moveActions = 3;
        final int resizeActions = 4;
        final Map<Long, ActionView> actionViewMap = new HashMap<>();

        LongStream.range(0, moveActions).forEach(i -> {
            final ActionView actionView = ActionOrchestratorTestUtils.createMoveAction(i, actionPlanId);
            actionViewMap.put(actionView.getId(), actionView);
        });

        // Need to add "moveActions" to the ID to avoid having ovelapping IDs.
        LongStream.range(0, resizeActions).map(actionNum -> moveActions + actionNum).forEach(i -> {
            final ActionView actionView = new Action(
                    ActionOrchestratorTestUtils.createResizeRecommendation(i, CommodityType.VMEM),
                    actionPlanId, actionModeCalculator, 124L);
            actionViewMap.put(actionView.getId(), actionView);
        });


        final ActionStore store = Mockito.mock(ActionStore.class);
        when(store.getActionViews()).thenReturn(new MapBackedActionViews(actionViewMap));

        when(actionStorehouse.getStore(eq(topologyContextId)))
            .thenReturn(Optional.of(store));

        final GetActionCountsResponse response = actionOrchestratorServiceClient.getActionCounts(
            GetActionCountsRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .build());

        Assert.assertEquals(moveActions + resizeActions, response.getTotal());
        // Should only have counts-by-type for the two action types.
        Assert.assertEquals(2, response.getCountsByTypeCount());
        assertThat(response.getCountsByTypeList(), containsInAnyOrder(
                TypeCount.newBuilder()
                    .setCount(moveActions)
                    .setType(ActionType.MOVE)
                    .build(),
                TypeCount.newBuilder()
                    .setCount(resizeActions)
                    .setType(ActionType.RESIZE)
                    .build()));
    }

    @Test
    public void testGetActionCountsWithQueryFilter() throws Exception {
        final ActionView firstAction = spy(ActionOrchestratorTestUtils.createMoveAction(1, actionPlanId));
        when(firstAction.getState()).thenReturn(ActionState.QUEUED);
        final ActionView secondAction = ActionOrchestratorTestUtils.createMoveAction(2, actionPlanId);
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
            firstAction.getId(), firstAction,
            secondAction.getId(), secondAction));
        when(actionStore.getActionViews()).thenReturn(actionViews);

        final ActionStore store = Mockito.mock(ActionStore.class);
        when(store.getActionViews()).thenReturn(actionViews);
        when(store.getVisibilityPredicate()).thenReturn(spec -> true);

        when(actionStorehouse.getStore(eq(topologyContextId)))
                .thenReturn(Optional.of(store));

        final GetActionCountsResponse response = actionOrchestratorServiceClient.getActionCounts(
                GetActionCountsRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setFilter(ActionQueryFilter.newBuilder()
                            .addStates(ActionState.QUEUED))
                        .build());

        // Only one QUEUED action.
        Assert.assertEquals(1, response.getTotal());
        // Should only have counts-by-type for the move action types.
        Assert.assertEquals(1, response.getCountsByTypeCount());
        assertThat(response.getCountsByTypeList(), contains(
                TypeCount.newBuilder()
                        .setCount(1)
                        .setType(ActionType.MOVE)
                        .build()));
    }

    @Test
    public void testGetActionCountsContextNotFound() throws Exception {
        when(actionStorehouse.getStore(eq(topologyContextId)))
                .thenReturn(Optional.empty());

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains(Long.toString(topologyContextId)));

        actionOrchestratorServiceClient.getActionCounts(
                GetActionCountsRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .build());
    }

    @Test
    public void testGetActionCountsByEntity() throws Exception {
        final long actionPlanId = 10;
        final ActionView visibleAction = spy(new Action(
                ActionOrchestratorTestUtils.createMoveRecommendation(1, 7, 77, 1, 777, 1),
                actionPlanId, actionModeCalculator, 234L));
        final ActionView invisibleAction = spy(new Action(ActionOrchestratorTestUtils.createMoveRecommendation(
                2, 8, 88, 1, 888, 1), actionPlanId, actionModeCalculator, 234L));
        final MapBackedActionViews actionViews = new MapBackedActionViews(ImmutableMap.of(
                visibleAction.getId(), visibleAction,
                invisibleAction.getId(), invisibleAction));
        when(actionStore.getActionViews()).thenReturn(actionViews);
        when(actionStore.getVisibilityPredicate()).thenReturn(view -> view.getId() == visibleAction.getId());

        final GetActionCountsByEntityResponse response =
                actionOrchestratorServiceClient.getActionCountsByEntity(
                        GetActionCountsByEntityRequest.newBuilder()
                                .setTopologyContextId(topologyContextId)
                                .setFilter(ActionQueryFilter.newBuilder()
                                    .setInvolvedEntities(InvolvedEntities.newBuilder()
                                        .addOids(7)
                                        .addOids(77)))
                                .build());

        Map<Long, List<TypeCount>> typeCountsByEntity = response.getActionCountsByEntityList().stream()
                .collect(Collectors.toMap(ActionCountsByEntity::getEntityId, ActionCountsByEntity::getCountsByTypeList));
        assertThat(typeCountsByEntity.size(), is(2));
        assertThat(typeCountsByEntity.get(7L), contains(TypeCount.newBuilder().setType(ActionType.MOVE).setCount(1).build()));
        assertThat(typeCountsByEntity.get(77L), contains(TypeCount.newBuilder().setType(ActionType.MOVE).setCount(1).build()));
    }

    @Test
    public void testGetActionCountsByEntityNoInvolvedEntities() throws Exception {

        final GetActionCountsByEntityResponse response =
            actionOrchestratorServiceClient.getActionCountsByEntity(
                GetActionCountsByEntityRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setFilter(ActionQueryFilter.newBuilder()
                                .setInvolvedEntities(InvolvedEntities.getDefaultInstance()))
                        .build());
        assertThat(response.getActionCountsByEntityCount(), is(0));
    }

    @Test
    public void testGetActionCountsByEntityContextNotFound() throws Exception {
        when(actionStorehouse.getStore(eq(topologyContextId)))
                .thenReturn(Optional.empty());

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(topologyContextId)));

        actionOrchestratorServiceClient.getActionCountsByEntity(
                GetActionCountsByEntityRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setFilter(ActionQueryFilter.newBuilder()
                                .setInvolvedEntities(InvolvedEntities.newBuilder()
                                        .addOids(7)))
                        .build());
    }

    @Test
    public void testGetActionCountsByDate() throws Exception {
        final long actionPlanId = 10;
        final LocalDateTime date = LocalDateTime.now();
        final LocalDateTime startOfDay = date.toLocalDate().atStartOfDay();
        final ActionView visibleAction = spy(new Action(ActionOrchestratorTestUtils.createMoveRecommendation(
                1, 7, 77, 1, 777, 1), actionPlanId, actionModeCalculator, 222L));
        when(visibleAction.getRecommendationTime())
               .thenReturn(date);
        final ActionView visibleQueuedAction = spy(new Action(ActionOrchestratorTestUtils.createMoveRecommendation(
                3, 7, 77, 1, 777, 1), actionPlanId, actionModeCalculator, 223L));
        when(visibleQueuedAction.getRecommendationTime())
                .thenReturn(date);
        when(visibleQueuedAction.getState())
                .thenReturn(ActionState.QUEUED);
        final ActionView invisibleAction = spy(new Action(ActionOrchestratorTestUtils.createMoveRecommendation(
                2, 8, 88, 1, 888, 1), actionPlanId, actionModeCalculator, 224L));
        final Map<Long, ActionView> actionViews = ImmutableMap.of(
                visibleAction.getId(), visibleAction,
                visibleQueuedAction.getId(), visibleQueuedAction,
                invisibleAction.getId(), invisibleAction);

        when(actionStore.getActionViews()).thenReturn(new MapBackedActionViews(actionViews,
            view -> view.getId() != invisibleAction.getId()));

        final GetActionCountsByDateResponse response =
                actionOrchestratorServiceClient.getActionCountsByDate(
                        GetActionCountsRequest.newBuilder()
                                .setTopologyContextId(topologyContextId)
                                .setFilter(ActionQueryFilter.newBuilder()
                                    .setVisible(true)
                                    .setStartDate(TimeUtil.localDateTimeToMilli(date, clock))
                                    .setEndDate(TimeUtil.localDateTimeToMilli(date, clock)))
                                .build());
        final Map<Long, List<StateAndModeCount>> countsByDate = response.getActionCountsByDateList().stream()
                .collect(Collectors.toMap(ActionCountsByDateEntry::getDate, ActionCountsByDateEntry::getCountsByStateAndModeList));

        assertThat(countsByDate.keySet(),
            // Start of day.
            is(Collections.singleton(TimeUtil.localDateTimeToMilli(startOfDay, clock))));
        assertThat(countsByDate.get(TimeUtil.localDateTimeToMilli(startOfDay, clock)), containsInAnyOrder(
            StateAndModeCount.newBuilder()
                .setState(ActionState.QUEUED)
                .setMode(ActionMode.RECOMMEND)
                .setCount(1)
                .build(),
            StateAndModeCount.newBuilder()
                .setState(ActionState.READY)
                .setMode(ActionMode.RECOMMEND)
                .setCount(1)
                .build()));
    }

    @Test
    public void testGetActionCountsByDateContextNotFound() throws Exception {
        when(actionStorehouse.getStore(eq(topologyContextId)))
                .thenReturn(Optional.empty());

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(topologyContextId)));

        actionOrchestratorServiceClient.getActionCountsByDate(
                GetActionCountsRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .build());
    }

    @Test
    public void testGetHistoricalActionStats() {
        final HistoricalActionStatsQuery query1 = HistoricalActionStatsQuery.newBuilder()
            .setGroupBy(GroupBy.ACTION_CATEGORY)
            .build();
        final HistoricalActionStatsQuery query2 = HistoricalActionStatsQuery.newBuilder()
            .setGroupBy(GroupBy.ACTION_STATE)
            .build();
        final ActionStats actionStats1 = ActionStats.newBuilder()
            .setMgmtUnitId(7)
            .build();
        final ActionStats actionStats2 = ActionStats.newBuilder()
            .setMgmtUnitId(8)
            .build();

        when(historicalStatReader.readActionStats(query1)).thenReturn(actionStats1);
        when(historicalStatReader.readActionStats(query2)).thenReturn(actionStats2);

        final GetHistoricalActionStatsResponse response =
            actionOrchestratorServiceClient.getHistoricalActionStats(
                GetHistoricalActionStatsRequest.newBuilder()
                    .addQueries(GetHistoricalActionStatsRequest.SingleQuery.newBuilder()
                        .setQueryId(1)
                        .setQuery(query1)
                        .build())
                    .addQueries(GetHistoricalActionStatsRequest.SingleQuery.newBuilder()
                        .setQueryId(2)
                        .setQuery(query2)
                        .build())
                    .build());

        verify(historicalStatReader).readActionStats(query1);
        verify(historicalStatReader).readActionStats(query2);
        final Map<Long, ActionStats> statsByQueryId = response.getResponsesList().stream()
            .collect(Collectors.toMap(resp -> resp.getQueryId(), resp -> resp.getActionStats()));
        assertThat(statsByQueryId.get(1L), is(actionStats1));
        assertThat(statsByQueryId.get(2L), is(actionStats2));
    }


    private List<ActionSpec> fetchSpecList(Iterable<ActionOrchestratorAction> rpcIter) {
        return StreamSupport.stream(rpcIter.spliterator(), false)
            .map(ActionOrchestratorAction::getActionSpec)
            .collect(Collectors.toList());
    }

    private List<ActionSpec> fetchSpecList(Iterator<FilteredActionResponse> responseIterator) {
        // check that first is pagination response
        assertThat(responseIterator.next().getTypeCase(), is(TypeCase.PAGINATION_RESPONSE));
        // collect all actions
        final List<ActionSpec> actionSpecs = new ArrayList<>();
        while (responseIterator.hasNext()) {
            actionSpecs.addAll(responseIterator.next().getActionChunk().getActionsList().stream()
                    .map(ActionOrchestratorAction::getActionSpec)
                    .collect(Collectors.toList()));
        }
        return actionSpecs;
    }

    @Nonnull
    private ActionSpec spec(@Nonnull final ActionView actionView) {
        return actionTranslator.translateToSpec(actionView);
    }
}
