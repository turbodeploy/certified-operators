package com.vmturbo.action.orchestrator.rpc;

import java.util.HashSet;
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

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.ExecutionStartException;
import com.vmturbo.action.orchestrator.execution.TargetResolutionException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextInfoRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceImplBase;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving and executing actions.
 */
public class ActionsRpcService extends ActionsServiceImplBase {

    private final ActionStorehouse actionStorehouse;

    private final ActionExecutor actionExecutor;

    private final ActionTranslator actionTranslator;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new ActionsRpcService.
     * @param actionStorehouse The storehouse containing action stores.
     * @param actionExecutor The executor for executing actions.
     * @param actionTranslator The translator for translating actions (from market to real-world).
     */
    public ActionsRpcService(@Nonnull final ActionStorehouse actionStorehouse,
                             @Nonnull final ActionExecutor actionExecutor,
                             @Nonnull final ActionTranslator actionTranslator) {
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionExecutor = Objects.requireNonNull(actionExecutor);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acceptAction(SingleActionRequest request,
                             StreamObserver<AcceptActionResponse> responseObserver) {
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

        AcceptActionResponse response = store.getAction(request.getActionId())
            .map(action -> {
                // TODO (roman, Sep 1, 2016): Set the user ID when we have it.
                AcceptActionResponse attemptResponse = attemptAcceptAndExecute(action, 0);
                if (!action.isReady()) {
                    store.getEntitySeverityCache()
                        .refresh(action.getRecommendation(), store);
                }

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
                              StreamObserver<ActionOrchestratorAction> responseObserver) {
        if (request.hasTopologyContextId()) {
            final Optional<ActionStore> store = actionStorehouse.getStore(request.getTopologyContextId());
            if (store.isPresent()) {
                Optional<ActionQueryFilter> filter = request.hasFilter() ?
                    Optional.of(request.getFilter()) :
                    Optional.empty();
                final Stream<ActionView> actionViews = new QueryFilter(filter)
                    .filteredActionViews(store.get());

                actionTranslator.translateToSpecs(actionViews)
                    .forEach(actionSpec -> responseObserver.onNext(aoAction(actionSpec)));
                responseObserver.onCompleted();
            } else {
                contextNotFoundError(responseObserver, request.getTopologyContextId());
            }
        } else {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Missing required parameter topologyContextId.")
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
            // TODO (roman, Feb 24 2017): Using "getActionSpecs" is unnecessarily expensive
            // because it's constructing a bunch of objects that quickly get discarded.
            // Ideally we can just iterate over the Action map directly, but the QueryFilter
            // (and visibility logic) currently only works with ActionSpecs.
            final Stream<ActionView> actionViewStream = request.hasFilter() ?
                    new QueryFilter(Optional.of(request.getFilter()))
                        .filteredActionViews(contextStore.get()) :
                    contextStore.get().getActionViews().values().stream();

            observeActionCounts(actionViewStream, response);
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
     * Attempt to accept and execute the action.
     *
     * @return The result of attempting to accept and execute the action.
     */
    private AcceptActionResponse attemptAcceptAndExecute(@Nonnull final Action action,
                                                                         final long userId) {
        long actionTargetId = -1;
        Optional<FailureEvent> failure = Optional.empty();

        try {
            actionTargetId = actionExecutor.getTargetId(action.getRecommendation());
        } catch (TargetResolutionException e) {
            logger.error("Failed to resolve target id for action {} due to error: {}", action.getId(), e);
            failure = Optional.of(new FailureEvent("Failed to resolve target id due to error: " + e.getMessage()));
        }

        if (action.receive(new ActionEvent.ManualAcceptanceEvent(userId, actionTargetId)).transitionNotTaken()) {
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
                actionExecutor.execute(targetId, translatedRecommendation.get());
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
        final Map<ActionType, Long> actionsByType = actionViewStream
            .map(ActionView::getRecommendation)
            .map(ActionDTO.Action::getInfo)
            .collect(Collectors.groupingBy(
                ActionDTOUtil::getActionInfoActionType,
                Collectors.counting()));

        final GetActionCountsResponse.Builder respBuilder = GetActionCountsResponse.newBuilder()
            .setTotal(actionsByType.values().stream()
                .mapToLong(count -> count)
                .sum());

        actionsByType.entrySet().stream()
            .map(typeCount -> GetActionCountsResponse.TypeCount.newBuilder()
                .setType(typeCount.getKey())
                .setCount(typeCount.getValue()))
            .forEach(respBuilder::addCountsByType);

        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
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
