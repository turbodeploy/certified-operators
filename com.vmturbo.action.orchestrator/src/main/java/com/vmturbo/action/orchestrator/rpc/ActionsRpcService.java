package com.vmturbo.action.orchestrator.rpc;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.protobuf.util.JsonFormat;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionPaginator;
import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionPaginator.PaginatedActionViews;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.approval.ActionApprovalManager;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.stats.HistoricalActionStatReader;
import com.vmturbo.action.orchestrator.stats.query.live.CurrentActionStatReader;
import com.vmturbo.action.orchestrator.stats.query.live.FailedActionQueryException;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.ActionStorehouse.StoreDeletionException;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.AcceptActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionStats;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.DeleteActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse.ActionChunk;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCategoryStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.ActionCountsByDateEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByDateResponse.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsByEntityResponse.ActionCountsByEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionCountsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetCurrentActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetHistoricalActionStatsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetInstanceIdsForRecommendationIdsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.MultiActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.RemoveActionsAcceptancesAndRejectionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.RemoveActionsAcceptancesAndRejectionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ResendAuditedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.ResendAuditedActionsResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.SingleActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.StateAndModeCount;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextInfoRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.TopologyContextResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.TypeCount;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceImplBase;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Implements the RPC calls supported by the action orchestrator for retrieving and executing actions.
 */
public class ActionsRpcService extends ActionsServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The storehouse containing action stores.
     */
    private final ActionStorehouse actionStorehouse;

    private final ActionApprovalManager actionApprovalManager;

    /**
     * To translate an action from the market's domain-agnostic form to the domain-specific form
     * relevant for execution and display in the real world.
     */
    private final ActionTranslator actionTranslator;

    /**
     * For paginating views of actions.
     */
    private final ActionPaginatorFactory paginatorFactory;


    private final HistoricalActionStatReader historicalActionStatReader;

    private final CurrentActionStatReader currentActionStatReader;

    private final Clock clock;

    private final UserSessionContext userSessionContext;

    private final AcceptedActionsDAO acceptedActionsStore;

    private final RejectedActionsDAO rejectedActionsStore;

    private final AuditedActionsManager auditedActionsManager;

    private final ActionAuditSender actionAuditSender;

    private final int actionPaginationMaxLimit;

    private final long realtimeTopologyContextId;

    /**
     * Create a new ActionsRpcService.
     *
     * @param clock the {@link Clock}
     * @param actionStorehouse the storehouse containing action stores.
     * @param actionApprovalManager action approval manager
     * @param actionTranslator the translator for translating actions (from market to real-world).
     * @param paginatorFactory for paginating views of actions
     * @param historicalActionStatReader reads stats of historical actions
     * @param currentActionStatReader reads stats of current actions
     * @param userSessionContext the user session context
     * @param acceptedActionsStore dao layer working with accepted actions
     * @param rejectedActionsStore dao layer working with rejected actions
     * @param auditedActionsManager object responsible for maintaining the book keeping
     * @param actionAuditSender receives and sends action events for audit
     * @param actionPaginationMaxLimit max number of actions to return in a single pagination page
     * @param realtimeTopologyContextId the ID of the topology context for realtime market analysis
     */
    public ActionsRpcService(@Nonnull final Clock clock,
            @Nonnull final ActionStorehouse actionStorehouse,
            @Nonnull final ActionApprovalManager actionApprovalManager,
            @Nonnull final ActionTranslator actionTranslator,
            @Nonnull final ActionPaginatorFactory paginatorFactory,
            @Nonnull final HistoricalActionStatReader historicalActionStatReader,
            @Nonnull final CurrentActionStatReader currentActionStatReader,
            @Nonnull final UserSessionContext userSessionContext,
            @Nonnull final AcceptedActionsDAO acceptedActionsStore,
            @Nonnull final RejectedActionsDAO rejectedActionsStore,
            @Nonnull final AuditedActionsManager auditedActionsManager,
            @Nonnull final ActionAuditSender actionAuditSender, final int actionPaginationMaxLimit,
            long realtimeTopologyContextId) {
        this.clock = clock;
        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.actionApprovalManager = Objects.requireNonNull(actionApprovalManager);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.paginatorFactory = Objects.requireNonNull(paginatorFactory);
        this.historicalActionStatReader = Objects.requireNonNull(historicalActionStatReader);
        this.currentActionStatReader = Objects.requireNonNull(currentActionStatReader);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.acceptedActionsStore = Objects.requireNonNull(acceptedActionsStore);
        this.rejectedActionsStore = Objects.requireNonNull(rejectedActionsStore);
        this.auditedActionsManager = Objects.requireNonNull(auditedActionsManager);
        this.actionAuditSender = Objects.requireNonNull(actionAuditSender);
        this.actionPaginationMaxLimit = actionPaginationMaxLimit;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void acceptAction(SingleActionRequest request,
                             StreamObserver<AcceptActionResponse> responseObserver) {
        String requestUserName = SecurityConstant.USER_ID_CTX_KEY.get();
        logger.debug("Getting action request from: " + requestUserName);
        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Missing required "
                + "parameter TopologyContextId").asException());
            return;
        }
        if (!request.hasActionId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Missing required "
                + "parameter ActionId").asException());
            return;
        }

        final Optional<ActionStore> optionalStore = actionStorehouse.getStore(request.getTopologyContextId());
        if (!optionalStore.isPresent()) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                "Unknown topology context: " + request.getTopologyContextId()).asException());
            return;
        }

        final ActionStore store = optionalStore.get();
        final Optional<Action> actionOpt = store.getAction(request.getActionId());

        if (!actionOpt.isPresent()) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                "Action " + request.getActionId() + " doesn't exist.").asException());
            return;
        }
        final Action action = actionOpt.get();

        // check if the action is manually scheduled and it does not have next occurrence and is
        // not currently active
        if (hasExpiredSchedule(action)) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Action " + request.getActionId()
                + " has execution window " + action.getSchedule().get().getScheduleDisplayName()
                + " which does not have a next occurrence. Therefore, the action cannot be "
                + "accepted").asException());
            return;
        }

        final String userNameAndUuid = AuditLogUtils.getUserNameAndUuidFromGrpcSecurityContext();

        final AcceptActionResponse response;
        try {
            response = actionApprovalManager.attemptAndExecute(store,
                    userNameAndUuid, action);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (ExecutionInitiationException e) {
            responseObserver.onError(e.toStatus());
        }
    }

    private boolean hasExpiredSchedule(@Nonnull Action action) {
        return action.getSchedule().isPresent()
            && action.getSchedule().get().getExecutionWindowActionMode() == ActionMode.MANUAL
            && action.getSchedule().get().getScheduleStartTimestamp() == null
            && !action.getSchedule().get().isActiveScheduleNow();
    }

    @Override
    public void getAction(SingleActionRequest request,
                          StreamObserver<ActionOrchestratorAction> responseObserver) {
        if (!request.hasActionId() || !request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing required parameter actionId or topologyContextId.")
                    .asException());
            return;
        }

        final Optional<ActionStore> storeOpt = actionStorehouse.getStore(request.getTopologyContextId());
        if (storeOpt.isPresent()) {
            final Optional<ActionSpec> optionalSpec = storeOpt.get()
                    .getActionView(request.getActionId())
                    .map(actionTranslator::translateToSpec);
            responseObserver.onNext(aoAction(request.getActionId(), optionalSpec));
            responseObserver.onCompleted();
        } else {
            actionStoreForContextNotFoundError(responseObserver, request.getTopologyContextId());
        }
    }

    @Override
    public void getAllActions(FilteredActionRequest request,
                              StreamObserver<FilteredActionResponse> responseObserver) {
        try {
            if (request.hasTopologyContextId()) {
                final Optional<ActionStore> store = actionStorehouse.getStore(request.getTopologyContextId());
                if (store.isPresent()) {
                    Tracing.log(() -> "Getting filtered actions. Request: " + JsonFormat.printer().print(request));
                    final ActionStore actionStore = store.get();
                    final ActionPaginator paginator = paginatorFactory.newPaginator();

                    // HashMap is specifically required because it allows a null key
                    final HashMap<ActionQuery, PaginatedActionViews> paginatedViewsByQuery =
                        new HashMap<>();
                    final List<ActionQuery> queries = request.getActionQueryList();
                    // We do translation after pagination because translation failures may
                    // succeed on retry. If we do translation before pagination, success after
                    // failure will mix up the pagination limit.
                    if (queries.isEmpty()) {
                        final Stream<ActionView> allViews = actionStore.getActionViews().getAll();
                        final PaginatedActionViews allViewsPaginated =
                            paginator.applyPagination(allViews, request.getPaginationParams());
                        paginatedViewsByQuery.put(null, allViewsPaginated);
                    }
                    queries.forEach(query -> {
                        final Stream<ActionView> filteredViews = query.hasQueryFilter()
                            ? actionStore.getActionViews().get(query.getQueryFilter())
                            : actionStore.getActionViews().getAll();
                        final PaginatedActionViews filteredViewsPaginated =
                            paginator.applyPagination(filteredViews, request.getPaginationParams());
                        paginatedViewsByQuery.put(query, filteredViewsPaginated);
                    });

                    paginatedViewsByQuery.forEach((query, paginatedViews) -> {
                        final PaginationResponse.Builder paginationResponseBuilder =
                            PaginationResponse.newBuilder()
                                .setTotalRecordCount(paginatedViews.getTotalRecordCount());
                        paginatedViews.getNextCursor()
                            .ifPresent(paginationResponseBuilder::setNextCursor);
                        // send pagination response as first in response
                        final FilteredActionResponse.Builder outerPaginationResponse =
                            FilteredActionResponse.newBuilder()
                                .setPaginationResponse(paginationResponseBuilder);
                        if (query != null && query.hasQueryId()) {
                            outerPaginationResponse.setQueryId(query.getQueryId());
                        }
                        responseObserver.onNext(outerPaginationResponse.build());
                        // stream results in current page in chunks
                        Lists.partition(paginatedViews.getResults(), actionPaginationMaxLimit)
                            .forEach(batch -> {
                                final ActionChunk.Builder actionChunk = ActionChunk.newBuilder();
                                actionTranslator.translateToSpecs(batch, actionStore)
                                    .map(ActionsRpcService::aoAction)
                                    .forEach(actionChunk::addActions);
                                final FilteredActionResponse.Builder outerActionResponse =
                                    FilteredActionResponse.newBuilder()
                                        .setActionChunk(actionChunk);
                                if (query != null && query.hasQueryId()) {
                                    outerActionResponse.setQueryId(query.getQueryId());
                                }
                                responseObserver.onNext(outerActionResponse.build());
                            });
                    });
                    responseObserver.onCompleted();
                } else {
                    actionStoreForContextNotFoundError(responseObserver, request.getTopologyContextId());
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

        Map<Long, ActionView> actionViews = optionalStore.get().getActionViews().get(actionIds)
            .collect(Collectors.toMap(ActionView::getId, Function.identity()));
        final Set<Long> contained = new HashSet<>(actionIds);
        contained.retainAll(actionViews.keySet()); // Contained (Intersection)
        actionIds.removeAll(actionViews.keySet()); // Missing (Difference)

        final Map<Long, ActionSpec> translatedActions = actionTranslator.translateToSpecs(contained.stream()
                .map(actionViews::get).collect(Collectors.toList()))
                .collect(Collectors.toMap(actionSpec -> actionSpec.getRecommendation().getId(), Function.identity()));

        // Get actionIds
        contained.forEach(containedId -> responseObserver.onNext(aoAction(translatedActions.get(containedId))));
        actionIds.forEach(missingId -> responseObserver.onNext(aoAction(missingId, Optional.empty())));
        responseObserver.onCompleted();
    }

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
            actionStoreForContextNotFoundError(response, request.getTopologyContextId());
        }
    }

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
                                    .atStartOfDay(), clock))); // Group by start of the Day
            observeActionCountsByDate(actionsByDate, response);
        } else {
            actionStoreForContextNotFoundError(response, request.getTopologyContextId());
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

        // verify access to the requested entities
        if (userSessionContext.isUserScoped()) {
            UserScopeUtils.checkAccess(userSessionContext,
                    request.getFilter().getInvolvedEntities().getOidsList());
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
                        ActionDTOUtil.getInvolvedEntityIds(actionView.getTranslationResultOrOriginal())
                                .stream()
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
            actionStoreForContextNotFoundError(response, request.getTopologyContextId());
        }
    }

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
                actionStoreForContextNotFoundError(responseObserver, contextId);
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

    @Override
    public void getActionCategoryStats(GetActionCategoryStatsRequest request,
                                       StreamObserver<GetActionCategoryStatsResponse> responseObserver) {

        Optional<ActionStore> actionStore =
                actionStorehouse.getStore(request.getTopologyContextId());
        if (!actionStore.isPresent()) {
            // Nothing to return
            responseObserver.onNext(GetActionCategoryStatsResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        Map<ActionCategory, ActionCategoryStats> actionCategoryStatsMap =
                new EnumMap<>(ActionCategory.class);
        Set<Integer> entityTypesToInclude = new HashSet<>();
        entityTypesToInclude.addAll(request.getEntityTypeList());
        // if no entity type set in request, include all entity types.
        if (entityTypesToInclude.isEmpty()) {
            for (EntityType type : EntityType.values()) {
                entityTypesToInclude.add(type.getNumber());
            }
        }

        for (ActionCategory actionCategory : ActionCategory.values()) {
            actionCategoryStatsMap.put(actionCategory, new ActionCategoryStats(entityTypesToInclude));
        }

        // group by {ActionCategory, numAction|numEntities, costPrice(savings|investment)}
        actionTranslator.translateToSpecs(actionStore.get().getActionViews().getAll().collect(Collectors.toList()))
            .forEach(actionSpec -> {
                final ActionCategory category = actionSpec.getCategory();
                if (category == ActionCategory.UNKNOWN) {
                    logger.error("Unknown action category for {}", actionSpec);
                } else {
                    actionCategoryStatsMap.get(category).addAction(actionSpec.getRecommendation());
                }
            });

        GetActionCategoryStatsResponse.Builder actionCategoryStatsResponse =
                GetActionCategoryStatsResponse.newBuilder();

        actionCategoryStatsMap.forEach((actionCategory, actionCategoryStats) -> {
            actionCategoryStatsResponse.addActionStatsByCategory(
                    ActionDTO.ActionCategoryStats.newBuilder()
                            .setActionCategory(actionCategory)
                            .setActionsCount(actionCategoryStats.getNumActions())
                            .setEntitiesCount(actionCategoryStats.getNumEntities())
                            .setSavings(actionCategoryStats.getTotalSavings())
                            .setInvestment(actionCategoryStats.getTotalInvestment())
                            .build());
        });
        responseObserver.onNext(actionCategoryStatsResponse.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getHistoricalActionStats(GetHistoricalActionStatsRequest request,
                                         StreamObserver<GetHistoricalActionStatsResponse> responseObserver) {
        final GetHistoricalActionStatsResponse.Builder respBuilder =
            GetHistoricalActionStatsResponse.newBuilder();
        // TODO: user scoping is not being applied here since the API is currently handling it. We
        // should probably move it here in the future though, since we are now doing other scope
        // checks in the Action Orchestrator
        for (GetHistoricalActionStatsRequest.SingleQuery query : request.getQueriesList()) {
            try {
                final ActionStats actionStats = historicalActionStatReader.readActionStats(query.getQuery());
                respBuilder.addResponses(GetHistoricalActionStatsResponse.SingleResponse.newBuilder()
                    .setQueryId(query.getQueryId())
                    .setActionStats(actionStats));
            } catch (DataAccessException e) {
                logger.error("Failed to read action stats for query {}. Error: {}",
                    query.getQuery(), e.getMessage());
                respBuilder.addResponses(GetHistoricalActionStatsResponse.SingleResponse.newBuilder()
                    .setQueryId(query.getQueryId())
                    .setError(e.getMessage())
                    .build());
            }
        }
        responseObserver.onNext(respBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getCurrentActionStats(GetCurrentActionStatsRequest request,
                                   StreamObserver<GetCurrentActionStatsResponse> responseObserver) {
        // NOTE: user scoping for action stats is handled in the API component.
        final GetCurrentActionStatsResponse.Builder respBuilder =
            GetCurrentActionStatsResponse.newBuilder();
        try {
            currentActionStatReader.readActionStats(request).entrySet().stream()
                .map(entry -> GetCurrentActionStatsResponse.SingleResponse.newBuilder()
                    .setQueryId(entry.getKey())
                    .addAllActionStats(entry.getValue()))
                .forEach(respBuilder::addResponses);

            responseObserver.onNext(respBuilder.build());
            responseObserver.onCompleted();
        } catch (FailedActionQueryException e) {
            logger.error("Current action query failed. Error: {}", e.getMessage());
            responseObserver.onError(e.asGrpcException());
        }
    }

    @Override
    public void removeActionsAcceptancesAndRejections(
            RemoveActionsAcceptancesAndRejectionsRequest request,
            StreamObserver<RemoveActionsAcceptancesAndRejectionsResponse> responseObserver) {
        try {
            acceptedActionsStore.removeAcceptanceForActionsAssociatedWithPolicy(
                    request.getPolicyId());
            rejectedActionsStore.removeRejectionsForActionsAssociatedWithPolicy(
                    request.getPolicyId());
            responseObserver.onNext(
                    RemoveActionsAcceptancesAndRejectionsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Failed to remove acceptances and rejections for actions associated with "
                    + "policy {}", request.getPolicyId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void resendAuditEvents(ResendAuditedActionsRequest request,
            StreamObserver<ResendAuditedActionsResponse> responseObserver) {
        try {
            if (!request.hasWorkflowId()) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                        "Missing required parameter 'workflowId'").asException());
                return;
            }
            final Optional<ActionStore> storeOpt = actionStorehouse.getStore(realtimeTopologyContextId);
            if (storeOpt.isPresent()) {
                final StopWatch stopWatch = new StopWatch();
                final ActionStore liveStore = storeOpt.get();
                final Collection<AuditedActionInfo> auditedActions =
                        auditedActionsManager.getAlreadySentActions(request.getWorkflowId());
                // actions which were sent for ON_GEN audit. We don't need to resend actions with
                // cleared_timestamp value because it means that they were cleared
                final Set<Long> actionsToResend = auditedActions.stream()
                        .filter(el -> !el.getClearedTimestamp().isPresent())
                        .map(AuditedActionInfo::getRecommendationId)
                        .collect(Collectors.toSet());
                // we can resend only actions which are still recommended by market
                final Set<ActionView> existedActionsToResend = liveStore.getActionViews()
                        .getByRecommendationId(actionsToResend)
                        .collect(Collectors.toSet());
                logger.info("Took {} ms to define {}/{} actual actions which will be resent for "
                                + "audit",
                        stopWatch.getTime(TimeUnit.MILLISECONDS), existedActionsToResend.size(),
                        auditedActions.size());
                final SetView<Long> notActualActions = Sets.difference(auditedActions.stream()
                        .map(AuditedActionInfo::getRecommendationId)
                        .collect(Collectors.toSet()), existedActionsToResend.stream()
                        .map(ActionView::getRecommendationOid)
                        .collect(Collectors.toSet()));
                if (!notActualActions.isEmpty()) {
                    logger.info(
                            "Actions ({}) weren't resent for audit because they are not recommended by market.",
                            notActualActions);
                }
                final int resendActionsCount = existedActionsToResend.isEmpty() ? 0
                        : actionAuditSender.resendActionEvents(existedActionsToResend);
                responseObserver.onNext(ResendAuditedActionsResponse.newBuilder()
                        .setAuditedActionsCount(resendActionsCount)
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL.withDescription(
                        "Live action store wasn't found. Operation of resending actions will be"
                                + " skipped").asException());
            }
        } catch (DataAccessException | CommunicationException | InterruptedException | UnsupportedActionException | ExecutionInitiationException ex) {
            responseObserver.onError(ex);
        }
    }

    @Override
    public void getInstanceIdsForRecommendationIds(GetInstanceIdsForRecommendationIdsRequest request,
                               StreamObserver<GetInstanceIdsForRecommendationIdsResponse> responseObserver) {
        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing required parameter topologyContextId.")
                    .asException());
            return;
        }

        final Optional<ActionStore> storeOpt = actionStorehouse.getStore(request.getTopologyContextId());
        if (storeOpt.isPresent()) {
            final ActionStore store = storeOpt.get();
            GetInstanceIdsForRecommendationIdsResponse.Builder builder =
                    GetInstanceIdsForRecommendationIdsResponse.newBuilder();
            request.getRecommendationIdList()
                    .stream()
                    .map(store::getActionByRecommendationId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(a -> builder.putRecommendationIdToInstanceId(a.getRecommendationOid(), a.getId()));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } else {
            actionStoreForContextNotFoundError(responseObserver, request.getTopologyContextId());
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
     * @param actionViewsMap   Key is date in long, value is its related actions.
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
     * actionStateList:["READY","ACCEPTED","SUCCEEDED"]
     * actionModeList:["MANUAL","AUTOMATIC","RECOMMEND"]
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
     * @param response       contains final relationship between entity with action type.
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
     * Utility method to query actions from an action store. We need to do this for all actions
     * that go out to the user.
     *
     * @param requestFilter An {@link Optional} containing the filter for actions.
     * @param actionStore   The action store to query.
     * @return A stream of post-translation {@link ActionView}s. Any actions that fail translation
     * will not be in this stream.
     */
    @Nonnull
    private Stream<ActionView> filteredTranslatedActionViews(Optional<ActionQueryFilter> requestFilter,
                                                             ActionStore actionStore) {
        QueryableActionViews actionViews = actionStore.getActionViews();
        return requestFilter.map(actionViews::get).orElseGet(actionViews::getAll);
    }

    private static Map<ActionType, Long> getActionsByType(@Nonnull final Stream<ActionView> actionViewStream) {
        return actionViewStream
                .map(ActionView::getTranslationResultOrOriginal)
                .collect(Collectors.groupingBy(
                        ActionDTOUtil::getActionInfoActionType,
                        Collectors.counting()));
    }


    private static void actionStoreForContextNotFoundError(@Nonnull final StreamObserver<?> responseObserver,
                                             final long topologyContextId) {
        responseObserver.onError(Status.NOT_FOUND
                .withDescription("Action store for context " + topologyContextId + " not found.")
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

    /**
     *  A helper class for accumulating the stats for each action category.
     */
    private class ActionCategoryStats {

        private int numActions;

        private Set<Long> entities;

        private double totalSavings;

        private double totalInvestment;

        Set<Integer> entityTypesToInclude;

        private ActionCategoryStats(@Nonnull  Set<Integer> entityTypesToInclude) {
            this.entities = new HashSet<>();
            this.entityTypesToInclude = Objects.requireNonNull(entityTypesToInclude);
        }

        public void addAction(@Nonnull final ActionDTO.Action action) {
            numActions++;
            try {
                ActionEntity targetEntity = ActionDTOUtil.getPrimaryEntity(action);
                if (entityTypesToInclude.contains(targetEntity.getType())) {
                    entities.add(targetEntity.getId());
                }
            } catch (UnsupportedActionException e) {
                logger.error("Exception while get targetEntityId from the action {}", action);
            }
            if (action.hasSavingsPerHour()) {
                double savings = action.getSavingsPerHour().getAmount();
                if (savings >= 0) {
                    totalSavings += savings;
                } else {
                    totalInvestment += Math.abs(action.getSavingsPerHour().getAmount());
                }
            }
        }

        public int getNumActions() {
            return numActions;
        }

        public int getNumEntities() {
            return entities.size();
        }

        public double getTotalSavings() {
            return totalSavings;
        }

        public double getTotalInvestment() {
            return totalInvestment;
        }

        @Override
        public int hashCode() {
            return Objects.hash(numActions, entities, totalSavings, totalInvestment);
        }

        @SuppressFBWarnings(
            value = "FE_FLOATING_POINT_EQUALITY",
            justification = "tolerance violates equals() transitive and hashCode() equals consistency contracts")
        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof ActionCategoryStats)) {
                return false;
            }
            ActionCategoryStats other = (ActionCategoryStats)obj;
            return (numActions == other.numActions
                    && Objects.equals(entities, other.entities)
                    && totalSavings == other.totalSavings
                    && totalInvestment == other.totalInvestment);
        }
    }
}
