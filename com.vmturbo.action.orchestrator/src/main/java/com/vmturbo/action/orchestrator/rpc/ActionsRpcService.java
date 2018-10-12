package com.vmturbo.action.orchestrator.rpc;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.PaginatedActionViews;
import com.vmturbo.action.orchestrator.action.ActionTypeToActionTypeCaseConverter;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetByProbeCategoryResolver;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.EntitiesResolutionException;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogEntry;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionProbePriorities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.ActionCountsByDateEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse.ActionCountsByEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionPrioritiesRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionPrioritiesResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.StateAndModeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextInfoRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceImplBase;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.components.api.TimeUtil;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving and executing actions.
 */
public class ActionsRpcService extends ActionsServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The storehouse containing action stores.
     */
    private final ActionStorehouse actionStorehouse;

    /**
     * To execute actions (by sending them to Topology Processor)
     */
    private final ActionExecutor actionExecutor;

    /**
     * For selecting which target/probe to execute each action against
     */
    private final ActionTargetSelector actionTargetSelector;

    /**
     * To translate an action from the market's domain-agnostic form to the domain-specific form
     * relevant for execution and display in the real world.
     */
    private final ActionTranslator actionTranslator;

    /**
     * For paginating views of actions
     */
    private final ActionPaginatorFactory paginatorFactory;

    /**
     * the store for all the known {@link WorkflowDTO.Workflow} items
     */
    private final WorkflowStore workflowStore;

    /**
     * Create a new ActionsRpcService.
     * @param actionStorehouse The storehouse containing action stores.
     * @param actionExecutor The executor for executing actions.
     * @param actionTargetSelector For selecting which target/probe to execute each action against
     * @param actionTranslator The translator for translating actions (from market to real-world).
     * @param paginatorFactory For paginating views of actions
     * @param workflowStore the store for all the known {@link WorkflowDTO.Workflow} items
     */
    public ActionsRpcService(@Nonnull final ActionStorehouse actionStorehouse,
                             @Nonnull final ActionExecutor actionExecutor,
                             @Nonnull final ActionTargetSelector actionTargetSelector,
                             @Nonnull final ActionTranslator actionTranslator,
                             @Nonnull final ActionPaginatorFactory paginatorFactory,
                             @Nonnull final WorkflowStore workflowStore) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.paginatorFactory = Objects.requireNonNull(paginatorFactory);
        this.workflowStore = Objects.requireNonNull(workflowStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acceptAction(SingleActionRequest request,
                             StreamObserver<AcceptActionResponse> responseObserver) {
        String requestUserName = SecurityConstant.USER_ID_CTX_KEY.get();
        logger.debug("Getting action request from: " + requestUserName);
        if (!request.hasTopologyContextId()) {
            responseObserver.onNext(acceptanceError("Missing required parameter TopologyContextId"));
            responseObserver.onCompleted();
            return;
        }
        if (!request.hasActionId()) {
            responseObserver.onNext(acceptanceError("Missing required parameter ActionId"));
            responseObserver.onCompleted();
            return;
        }

        final Optional<ActionStore> optionalStore = actionStorehouse.getStore(request.getTopologyContextId());
        if (!optionalStore.isPresent()) {
            responseObserver.onNext(acceptanceError("Unknown topology context: " + request.getTopologyContextId()));
            responseObserver.onCompleted();
            return;
        }

        final ActionStore store = optionalStore.get();
        final String userNameAndUuid = AuditLogUtils.getUserNameAndUuidFromGrpcSecurityContext();
        AcceptActionResponse response = store.getAction(request.getActionId())
            .map(action -> {
                AcceptActionResponse attemptResponse = attemptAcceptAndExecute(action,
                        userNameAndUuid);
                if (!action.isReady()) {
                    store.getEntitySeverityCache()
                        .refresh(action.getRecommendation(), store);
                }
                AuditLogEntry entry = new AuditLogEntry.Builder(AuditAction.EXECUTE_ACTION, action.toString(), true)
                        .targetName(String.valueOf(action.getId()))
                        .build();
                AuditLogUtils.audit(entry);
                return attemptResponse;
            }).orElse(acceptanceError("Action " + request.getActionId() + " doesn't exist."));



        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getAction(SingleActionRequest request,
                          StreamObserver<ActionOrchestratorAction> responseObserver) {
        if (!request.hasActionId() || !request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing required parameter actionId or topologyContextId.")
                .asException());
            return;
        }

        final Optional<ActionStore> store = actionStorehouse.getStore(request.getTopologyContextId());
        if (store.isPresent()) {
            final Optional<ActionSpec> optionalSpec = store.get()
                .getActionView(request.getActionId())
                .map(actionTranslator::translateToSpec);

            responseObserver.onNext(aoAction(request.getActionId(), optionalSpec));
            responseObserver.onCompleted();
        } else {
            contextNotFoundError(responseObserver, request.getTopologyContextId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getAllActions(FilteredActionRequest request,
                              StreamObserver<FilteredActionResponse> responseObserver) {
        try {
            if (request.hasTopologyContextId()) {
                final Optional<ActionStore> store = actionStorehouse.getStore(request.getTopologyContextId());
                if (store.isPresent()) {
                    Optional<ActionQueryFilter> filter = request.hasFilter() ?
                            Optional.of(request.getFilter()) :
                            Optional.empty();
                    // We do translation after pagination because translation failures may
                    // succeed on retry. If we do translation before pagination, success after
                    // failure will mix up the pagination limit.
                    final Stream<ActionView> resultViews = new QueryFilter(filter)
                            .filteredActionViews(store.get());

                    final FilteredActionResponse.Builder responseBuilder =
                            FilteredActionResponse.newBuilder()
                                    .setPaginationResponse(PaginationResponse.newBuilder());

                    final PaginatedActionViews paginatedViews = paginatorFactory.newPaginator()
                            .applyPagination(resultViews, request.getPaginationParams());

                    paginatedViews.getNextCursor().ifPresent(nextCursor ->
                            responseBuilder.getPaginationResponseBuilder().setNextCursor(nextCursor));

                    actionTranslator.translateToSpecs(paginatedViews.getResults().stream())
                            .map(ActionsRpcService::aoAction)
                            .forEach(responseBuilder::addActions);

                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                } else {
                    contextNotFoundError(responseObserver, request.getTopologyContextId());
                }
            } else {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Missing required parameter topologyContextId.")
                        .asException());
            }
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getLocalizedMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getActions(MultiActionRequest request,
                           StreamObserver<ActionOrchestratorAction> responseObserver) {
        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing required parameter topologyContextId.")
                .asException());
            return;
        }

        // This is a bit complicated for controller logic, but it would really never
        // be reused anywhere else.
        final Set<Long> actionIds = Sets.newHashSet(request.getActionIdsList());
        Optional<ActionStore> optionalStore = actionStorehouse.getStore(request.getTopologyContextId());
        if (!optionalStore.isPresent()) {
            // Nothing to return
            responseObserver.onCompleted();
            return;
        }

        final Map<Long, ActionView> actionViews = optionalStore.get().getActionViews();
        final Set<Long> contained = new HashSet<>(actionIds);
        contained.retainAll(actionViews.keySet()); // Contained (Intersection)
        actionIds.removeAll(actionViews.keySet()); // Missing (Difference)

        final Map<Long, ActionSpec> translatedActions = actionTranslator.translateToSpecs(contained.stream()
                .map(actionViews::get))
                .collect(Collectors.toMap(actionSpec -> actionSpec.getRecommendation().getId(), Function.identity()));

        // Get actionIds
        contained.forEach(containedId -> responseObserver.onNext(aoAction(translatedActions.get(containedId))));
        actionIds.forEach(missingId -> responseObserver.onNext(aoAction(missingId, Optional.empty())));
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getTopologyContextInfo(TopologyContextInfoRequest request,
                                  StreamObserver<TopologyContextResponse> responseObserver) {
        actionStorehouse.getAllStores().entrySet()
            .forEach(contextIdAndStore -> {
                final ActionStore store = contextIdAndStore.getValue();
                responseObserver.onNext(TopologyContextResponse.newBuilder()
                        .setTopologyContextId(contextIdAndStore.getKey())
                        .setActionCount(store.size()).build()
                );
            });

        responseObserver.onCompleted();
    }

    @Override
    public void getActionCounts(GetActionCountsRequest request,
                                StreamObserver<GetActionCountsResponse> response) {
        final Optional<ActionStore> contextStore =
                actionStorehouse.getStore(request.getTopologyContextId());
        if (contextStore.isPresent()) {
            final Stream<ActionView> translatedActionViews = filteredTranslatedActionViews(
                    request.hasFilter() ? Optional.of(request.getFilter()) : Optional.empty(),
                    contextStore.get());
            observeActionCounts(translatedActionViews, response);
        } else {
            contextNotFoundError(response, request.getTopologyContextId());
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void getActionCountsByDate(GetActionCountsRequest request,
                                StreamObserver<GetActionCountsByDateResponse> response) {
        final Optional<ActionStore> contextStore =
                actionStorehouse.getStore(request.getTopologyContextId());
        if (contextStore.isPresent()) {
            // Get actions within the startDate and endDate, filter the relevant actions, and
            // group the actions by recommended date.
            final Stream<ActionView> translatedActionViews = filteredTranslatedActionViews(
                    request.hasFilter() ? Optional.of(request.getFilter()) : Optional.empty(),
                    contextStore.get());
            final Map<Long, List<ActionView>> actionsByDate =
                translatedActionViews.collect(Collectors.groupingBy(action ->
                    TimeUtil.localDateTimeToMilli(action
                        .getRecommendationTime()
                        .toLocalDate()
                        .atStartOfDay()))); // Group by start of the Day
            observeActionCountsByDate(actionsByDate, response);
        } else {
            contextNotFoundError(response, request.getTopologyContextId());
        }
    }


    @Override
    public void getActionCountsByEntity(GetActionCountsByEntityRequest request,
                                        StreamObserver<GetActionCountsByEntityResponse> response) {
        if (!request.hasFilter() || !request.getFilter().hasInvolvedEntities()) {
            response.onError(Status.INVALID_ARGUMENT
                .withDescription("Get action counts by entity need provide a action filter and entities.")
                .asException());
        }
        final Optional<ActionStore> contextStore =
            actionStorehouse.getStore(request.getTopologyContextId());
        if (contextStore.isPresent()) {
            final Multimap<Long, ActionView> actionsByEntity = ArrayListMultimap.create();
            // If there are no involved entities in the request we return an empty map.
            if (!request.getFilter().getInvolvedEntities().getOidsList().isEmpty()) {
                final Stream<ActionView> translatedActionViews = filteredTranslatedActionViews(
                        request.hasFilter() ? Optional.of(request.getFilter()) : Optional.empty(),
                        contextStore.get());

                final Set<Long> targetEntities =
                        new HashSet<>(request.getFilter().getInvolvedEntities().getOidsList());
                // Collect action views by entity ID of the involved entities.
                // For example: Action 1: Move VM1 from Host1 to Host2, and input entity Id is Host 2.
                // In the final Map, there will be one entry: key Host2 and Value is Action 1.
                translatedActionViews.forEach(actionView -> {
                    try {
                        ActionDTOUtil.getInvolvedEntities(actionView.getRecommendation()).stream()
                                // We only care about actions that involve the target entities.
                                .filter(targetEntities::contains)
                                .forEach(entityId -> actionsByEntity.put(entityId, actionView));
                    } catch (UnsupportedActionException e) {
                        // if action not supported, ignore this action
                        logger.warn("Unsupported action {}", actionView);
                    }
                });
            }
            observeActionCountsByEntity(actionsByEntity, response);
        } else {
            contextNotFoundError(response, request.getTopologyContextId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteActions(DeleteActionsRequest request,
                              StreamObserver<DeleteActionsResponse> responseObserver) {
        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing required parameter topologyContextId.")
                .asException());
            return;
        }

        final long contextId = request.getTopologyContextId();
        try {
            // Get the store size before deleting because the deleting clears the store.
            final int storeSize = actionStorehouse.getStore(contextId)
                .map(ActionStore::size)
                .orElse(-1);
            final Optional<ActionStore> actionStore = actionStorehouse.deleteStore(contextId);

            if (actionStore.isPresent()) {
                responseObserver.onNext(DeleteActionsResponse.newBuilder()
                        .setTopologyContextId(contextId)
                        .setActionCount(storeSize)
                        .build());
                responseObserver.onCompleted();
            } else {
                contextNotFoundError(responseObserver, contextId);
            }
        } catch (IllegalStateException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Operation not permitted for context " + contextId)
                .asException());
        } catch (StoreDeletionException e) {
            responseObserver.onError(Status.INTERNAL
                .withDescription("Attempt to delete actions for context " + contextId + " failed.")
                .asException());
        }
    }

    /**
     * Sets to responseObserver probe priorities for the certain action type if request
     * contains this type. Otherwise sets probe priorities for all types.
     *
     * @param request may contain the certain action type.
     * @param responseObserver to set probe priorities in.
     */
    @Override
    @Nonnull
    public void getActionPriorities(@Nonnull GetActionPrioritiesRequest request,
            @Nonnull StreamObserver<GetActionPrioritiesResponse> responseObserver) {
        final GetActionPrioritiesResponse.Builder responseBuilder =
                GetActionPrioritiesResponse.newBuilder();
        if (request.hasActionType()) {
            responseBuilder.addActionProbePriorities(getProbePrioritiesFor(request.getActionType()));
        } else {
            addAllActionProbePrioritiesToResponse(responseBuilder);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void cancelQueuedActions(
            CancelQueuedActionsRequest request,
            StreamObserver<CancelQueuedActionsResponse> responseObserver) {

        responseObserver.onNext(
                CancelQueuedActionsResponse.newBuilder()
                    .setCancelledCount(
                            actionStorehouse.cancelQueuedActions())
                    .build());
        responseObserver.onCompleted();
    }

    private void addAllActionProbePrioritiesToResponse(
            @Nonnull GetActionPrioritiesResponse.Builder responseBuilder) {
        for (ActionType actionType : ActionType.values()) {
            responseBuilder.addActionProbePriorities(getProbePrioritiesFor(actionType));
        }
    }

    @Nonnull
    private ActionProbePriorities getProbePrioritiesFor(@Nonnull ActionType actionType) {
        final ActionTypeCase actionTypeCase = ActionTypeToActionTypeCaseConverter
                .getActionTypeCaseFor(actionType);
        final List<String> probePriorities =  ActionTargetByProbeCategoryResolver
                .getProbePrioritiesFor(actionTypeCase);
        return ActionProbePriorities.newBuilder().setActionType(actionType)
                .addAllProbeCategory(probePriorities).build();
    }

    /**
     * Attempt to accept and execute the action.
     *
     * @return The result of attempting to accept and execute the action.
     */
    private AcceptActionResponse attemptAcceptAndExecute(@Nonnull final Action action,
                                                         final String userUUid) {
        long actionTargetId = -1;
        Optional<FailureEvent> failure = Optional.empty();

        try {
            actionTargetId = actionTargetSelector.getTargetId(action.getRecommendation());
        } catch (TargetResolutionException | UnsupportedActionException | EntitiesResolutionException e) {
            logger.error("Failed to resolve target id for action {} due to error: {}", action.getId(), e);
            failure = Optional.of(new FailureEvent("Failed to resolve target id due to error: " + e.getMessage()));
        }

        if (action.receive(new ActionEvent.ManualAcceptanceEvent(userUUid, actionTargetId)).transitionNotTaken()) {
            return acceptanceError("Unauthorized to accept action in mode " + action.getMode());
        }

        return handleTargetResolution(action, actionTargetId, failure);
    }

    private AcceptActionResponse handleTargetResolution(@Nonnull final Action action,
                                                        final long targetId,
                                                        @Nonnull final Optional<FailureEvent> failure) {
        return failure
            .map(failureEvent -> {
                action.receive(failureEvent);
                return acceptanceError(failureEvent.getErrorDescription());
                // TODO (roman, Sep 1, 2016): Figure out criteria for when to begin execution
            }).orElseGet(() -> attemptActionExecution(action, targetId));
    }

    private AcceptActionResponse attemptActionExecution(@Nonnull final Action action,
                                                        final long targetId) {
        try {
            action.receive(new BeginExecutionEvent());
            actionTranslator.translate(action);
            final Optional<ActionDTO.Action> translatedRecommendation =
                action.getActionTranslation().getTranslatedRecommendation();
            if (translatedRecommendation.isPresent()) {
                // execute the action, passing the workflow override (if any)
                actionExecutor.execute(targetId, translatedRecommendation.get(),
                        action.getWorkflow(workflowStore));
                return AcceptActionResponse.newBuilder()
                    .setActionSpec(actionTranslator.translateToSpec(action))
                    .build();
            } else {
                final String errorMsg = String.format("Failed to translate action %d for execution.", action.getId());
                logger.error(errorMsg);
                action.receive(new FailureEvent(errorMsg));
                return acceptanceError(errorMsg);
            }
        } catch (ExecutionStartException e) {
            logger.error("Failed to start action {} due to error {}.", action.getId(), e);
            action.receive(new FailureEvent(e.getMessage()));
            return acceptanceError(e.getMessage());
        }
    }

    static void observeActionCounts(@Nonnull final Stream<ActionView> actionViewStream,
                                     @Nonnull final StreamObserver<GetActionCountsResponse> responseObserver) {
        final Map<ActionType, Long> actionsByType = getActionsByType(actionViewStream);

        final GetActionCountsResponse.Builder respBuilder = GetActionCountsResponse.newBuilder()
            .setTotal(actionsByType.values().stream()
                .mapToLong(count -> count)
                .sum());

        actionsByType.entrySet().stream()
            .map(typeCount -> TypeCount.newBuilder()
                .setType(typeCount.getKey())
                .setCount(typeCount.getValue()))
            .forEach(respBuilder::addCountsByType);

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }


    /**
     * Count action types for each available date.
     *
     * @param actionViewsMap Key is date in long, value is its related actions.
     * @param responseObserver contains final relationship between date with action type.
     */
    static void observeActionCountsByDate(@Nonnull final Map<Long, List<ActionView>> actionViewsMap,
                                    @Nonnull final StreamObserver<GetActionCountsByDateResponse> responseObserver) {
        Builder respBuilder = getActionCountsByDateResponseBuilder(actionViewsMap);
        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * Build action state and mode counts by date.
     * actionStateList:["PENDING_ACCEPT","RECOMMENDED","ACCEPTED","SUCCEEDED"]
     * actionModeList:["MANUAL","AUTOMATIC","RECOMMEND"
     *
     * @param actionViewsMap key is date(long type), and value is list of actions recommended on the date.
     * @return builder with {@link ActionCountsByDateEntry} objects. In {@link ActionCountsByDateEntry}
     * object, key is the date (long type), and value is list of {@link StateAndModeCount}.
     */
    @VisibleForTesting
    public static Builder getActionCountsByDateResponseBuilder(final @Nonnull Map<Long, List<ActionView>> actionViewsMap) {
        Builder respBuilder = GetActionCountsByDateResponse.newBuilder();
        // composite key for action state and mode.
        Function<ActionView, List<Object>> compositeKey = action ->
                Arrays.<Object>asList(action.getState(), action.getMode());

        for (Map.Entry<Long, List<ActionView>> entry : actionViewsMap.entrySet()) {
            Map<List<Object>, List<ActionView>> stateAndModeActionsMap =
                    entry.getValue().stream().collect(Collectors.groupingBy(compositeKey, Collectors.toList()));
            ActionCountsByDateEntry.Builder actionCountsByDateEntry = ActionCountsByDateEntry.newBuilder();
            actionCountsByDateEntry.setDate(entry.getKey());
            stateAndModeActionsMap.entrySet().stream()
                    .map(stateAndModeEntry -> StateAndModeCount.newBuilder()
                            .setState(ActionState.valueOf(stateAndModeEntry.getKey().get(0).toString()))
                            .setMode(ActionMode.valueOf(stateAndModeEntry.getKey().get(1).toString()))
                            .setCount(stateAndModeEntry.getValue().size()))
                    .forEach(
                            builder -> actionCountsByDateEntry.addCountsByStateAndMode(builder));
            respBuilder.addActionCountsByDate(actionCountsByDateEntry);
        }
        return respBuilder;
    }

    /**
     * Count action type for each entity. And for one entity, it could have multiple action type and
     * for each action type, it could have a few actions.
     *
     * @param actionViewsMap Key is entity Id, value is its related action views.
     * @param response contains final relationship between entity with action type.
     */
    static void observeActionCountsByEntity(@Nonnull final Multimap<Long, ActionView> actionViewsMap,
                                            @Nonnull StreamObserver<GetActionCountsByEntityResponse> response) {
        GetActionCountsByEntityResponse.Builder actionCountsByEntityResponseBuilder =
            GetActionCountsByEntityResponse.newBuilder();
        actionViewsMap.asMap().entrySet().stream()
            .forEach(entry -> {
                // TODO: implement more group mechanism such as group by mode, state.
                final Map<ActionType, Long> actionsByType = getActionsByType(entry.getValue().stream());

                final ActionCountsByEntity.Builder actionCountsByEntityBuilder = ActionCountsByEntity.newBuilder()
                    .setEntityId(entry.getKey());

                actionsByType.entrySet().stream()
                    .map(typeCount -> TypeCount.newBuilder()
                        .setType(typeCount.getKey())
                        .setCount(typeCount.getValue()))
                    .forEach(actionCountsByEntityBuilder::addCountsByType);
                actionCountsByEntityResponseBuilder.addActionCountsByEntity(actionCountsByEntityBuilder.build());
            });
        response.onNext(actionCountsByEntityResponseBuilder.build());
        response.onCompleted();
    }

    /**
     * Utility method to query actions from an action store and apply translation. We need to
     * do this for all actions that go out to the user.
     *
     * @param requestFilter An {@link Optional} containing the filter for actions.
     * @param actionStore The action store to query.
     * @return A stream of post-translation {@link ActionView}s. Any actions that fail translation
     *         will not be in this stream.
     */
    @Nonnull
    private Stream<ActionView> filteredTranslatedActionViews(Optional<ActionQueryFilter> requestFilter,
                                                             ActionStore actionStore) {
        return actionTranslator.translate(new QueryFilter(requestFilter)
                .filteredActionViews(actionStore));
    }

    private static Map<ActionType, Long> getActionsByType(@Nonnull final Stream<ActionView> actionViewStream) {
        return actionViewStream
            .map(ActionView::getRecommendation)
            .collect(Collectors.groupingBy(
                ActionDTOUtil::getActionInfoActionType,
                Collectors.counting()));
    }

    private static AcceptActionResponse acceptanceError(@Nonnull final String error) {
        return AcceptActionResponse.newBuilder()
            .setError(error)
            .build();
    }

    private static void contextNotFoundError(@Nonnull final StreamObserver<?> responseObserver,
                                             final long topologyContextId) {
        responseObserver.onError(Status.NOT_FOUND
            .withDescription("Context: " + topologyContextId + " not found.")
            .asException());
    }

    private static ActionOrchestratorAction aoAction(final long actionId,
                                                     final Optional<ActionSpec> optionalSpec) {
        ActionOrchestratorAction.Builder builder = ActionOrchestratorAction.newBuilder()
            .setActionId(actionId);
        optionalSpec.ifPresent(builder::setActionSpec);

        return builder.build();
    }

    private static ActionOrchestratorAction aoAction(@Nonnull final ActionSpec spec) {
        return ActionOrchestratorAction.newBuilder()
            .setActionId(spec.getRecommendation().getId())
            .setActionSpec(spec)
            .build();
    }
}
