package com.vmturbo.cost.component.savings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Listens for events from the action orchestrator and inserts events into the internal
 * savings event log.  Interesting event types are: new recommendation and action successfully
 * executed.
 */
public class ActionListener implements ActionsListener {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Action State Map.
     */
    private final Map<Long, ActionState> entityActionStateMap = new ConcurrentHashMap<>();

    /**
     * The In Memory Events Journal.
     */
    private final EntityEventsJournal entityEventsJournal;

    /**
     * The Map of current (active action) actionSpec to entityId.
     *
     * <p>This mapping is preserved in order to process stale action events.
     */
    private final Map<ActionSpec, Long> currentPendingActionSpecsToEntityId = new ConcurrentHashMap<>();

    /**
     * For making Grpc calls to Action Orchestrator.
     */
    private final ActionsServiceBlockingStub actionsService;

    /**
     * For making Grpc calls to Cost.
     */
    private final CostServiceBlockingStub costService;

    /**
     * Real-time context id.
     */
    private final Long realTimeTopologyContextId;

    /**
     * Pending Action Types.
     */
    private final Set<ActionType> pendingActionTypes;

    /**
     * Pending Action MODES. (Maybe we need to check if executable ? )
     */
    private final ImmutableSet<ActionMode> pendingActionModes = ImmutableSet.of(ActionMode.MANUAL,
                                                                                ActionMode.RECOMMEND);
    /**
     * Pending Action States.
     */
    private final ImmutableSet<ActionState> pendingActionStates = ImmutableSet.of(ActionState.READY);
    /**
     * Pending Action Entity Types.
     */
    private final Set<Integer> pendingWorkloadTypes;

    /**
     * Map of entity type to a set of cost categories for which costs need to be queried for.
     */
    private static final Map<Integer, Set<CostCategory>> costCategoriesByEntityType = new HashMap<>();

    static {
        costCategoriesByEntityType.put(EntityType.VIRTUAL_MACHINE_VALUE,
                ImmutableSet.of(CostCategory.ON_DEMAND_COMPUTE, CostCategory.ON_DEMAND_LICENSE));
        costCategoriesByEntityType.put(EntityType.VIRTUAL_VOLUME_VALUE,
                ImmutableSet.of(CostCategory.STORAGE));
        costCategoriesByEntityType.put(EntityType.DATABASE_VALUE,
                ImmutableSet.of(CostCategory.ON_DEMAND_COMPUTE, CostCategory.STORAGE));
    }

    /**
     * Convenience to represent 0 costs, to avoid recreating it each time.
     */
    private final CurrencyAmount zeroCosts = CurrencyAmount.newBuilder().setAmount(0d).build();

    /**
     * Convenience to represent empty price change, to avoid recreating it each time.
     */
    private final EntityPriceChange emptyPriceChange = new EntityPriceChange.Builder()
            .sourceOid(0L)
            .sourceCost(0.0)
            .destinationOid(0L)
            .destinationCost(0.0)
            .build();

    /**
     * Action lifetimes.
     */
    private final Long actionLifetimeMs;
    private final Long deleteVolumeActionLifetimeMs;

    /**
     * Constructor.
     *
     * @param entityEventsInMemoryJournal Entity Events Journal to maintain Savings events including those related to actions.
     * @param actionsServiceBlockingStub Stub for Grpc calls to actions service.
     * @param costServiceBlockingStub Stub for Grpc calls to cost service.
     * @param realTimeContextId The real-time topology context id.
     * @param supportedEntityTypes Set of entity types supported.
     * @param supportedActionTypes Set of action types supported.
     * @param actionLifetimeMs lifetime in ms for all actions other than delete volume
     * @param deleteVolumeActionLifetimeMs lifetime in ms for delete volume actions
     */
    ActionListener(@Nonnull final EntityEventsJournal entityEventsInMemoryJournal,
                    @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                    @Nonnull CostServiceBlockingStub costServiceBlockingStub,
                    @Nonnull final Long realTimeContextId,
                    @Nonnull Set<EntityType> supportedEntityTypes,
                    @Nonnull Set<ActionType> supportedActionTypes,
                    @Nonnull final Long actionLifetimeMs,
                    @Nonnull final Long deleteVolumeActionLifetimeMs) {
        entityEventsJournal = Objects.requireNonNull(entityEventsInMemoryJournal);
        actionsService = Objects.requireNonNull(actionsServiceBlockingStub);
        costService = Objects.requireNonNull(costServiceBlockingStub);
        realTimeTopologyContextId = realTimeContextId;
        pendingWorkloadTypes = supportedEntityTypes.stream()
                .map(EntityType::getNumber)
                .collect(Collectors.toSet());
        pendingActionTypes = supportedActionTypes;
        this.actionLifetimeMs = Objects.requireNonNull(actionLifetimeMs);
        this.deleteVolumeActionLifetimeMs = Objects.requireNonNull(deleteVolumeActionLifetimeMs);
    }

    /**
     * A notification sent by the Topology Processor to report the successful
     * completion of an action being executed. This notification will be triggered
     * when the Topology Processor receives the corresponding success update from
     * a probe.
     *
     * <p>Process Successfully executed actions, and add EXECUTION_ADDED events along with price change
     * information to Events Journal.
     * @param actionSuccess The progress notification for an action.
     */
    @Override
    public void onActionSuccess(@Nonnull ActionSuccess actionSuccess) {
        // Locate the target entity in the internal entity state.  If not present, create an
        //  - entry for it.
        //  - Log a provider change event into the event log.
        // TODO: The way we're processing update and success, we may not need synchronization.
        // However this may need to be re-evaluated in the future, because of potential race conditions,
        // in the presence of multiple threads.
        final Long actionId = actionSuccess.getActionId();
        ActionState prevActionState = entityActionStateMap.get(actionId);
        // Check if a SUCCEEDED for the action id hasn't already been added, in order to
        // to avoid processing more than once if multiple SUCCEEDED notifications were to be received.
        // There could be a previous READY/PENDING_ACCEPT entry or not entry, and that's fine.
        if (prevActionState != ActionState.SUCCEEDED) {
            logger.info("Action {} changed from {} to SUCCEEDED", actionId, prevActionState);
            // Add a Succeeded Action event to the Events Journal with time-stamp as the completion time.
            ActionEntity entity;
            try {
                final ActionSpec actionSpec = actionSuccess.getActionSpec();
                entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
                if (pendingWorkloadTypes.contains(entity.getType())) {
                    final Long completionTime = actionSpec.getExecutionStep().getCompletionTime();
                    final Long entityId = entity.getId();
                    Map<Long, EntityActionInfo> entityIdToActionInfoMap = new HashMap<>();
                    entityIdToActionInfoMap.put(entityId, getEntityActionInfo(actionId, actionSpec,
                            entity));
                    final Map<Long, EntityPriceChange> entityPriceChangeMap = getEntityCosts(
                            entityIdToActionInfoMap);
                    final EntityPriceChange actionPriceChange = entityPriceChangeMap.get(actionId);
                    if (actionPriceChange != null) {
                        EntityPriceChange actionPriceChangeWithExpiration =
                                new EntityPriceChange.Builder()
                                        .from(actionPriceChange)
                                        .expirationTime(Optional.of(completionTime + actionLifetimeMs))
                                        .build();
                        final SavingsEvent successEvent = createActionEvent(entity.getId(),
                                completionTime,
                                ActionEventType.EXECUTION_SUCCESS,
                                actionId,
                                actionPriceChangeWithExpiration);
                        entityEventsJournal.addEvent(successEvent);
                        entityActionStateMap.put(actionId, ActionState.SUCCEEDED);
                        logger.debug("Added action {} for entity {}, completion time {}, recommendation time {}, journal size {}",
                                    actionId, entityId, completionTime,
                                    actionSpec.getRecommendationTime(),
                                    entityEventsJournal.size());
                    }
                }
            } catch (UnsupportedActionException e) {
                logger.error("Cannot create action Savings event due to unsupported action type",
                            e);
            }
        }
    }

    /**
     * Callback when the actions stored in the ActionOrchestrator have been updated. Replaces the
     * "onActionsReceived" event.
     *
     * <p>Process new Pending VM Scale actions and add to RECOMMENDATION_ADDED events to Events Journal.
     * Process stale Pending VM Scale actions and add to RECOMMENDATION_REMOVED events to Events Journal.
     * Retrieve on-demand cost before and after for each action and populate the Savings Events with this information.
     *
     * @param actionsUpdated Context containing topology and action plan information.
     */
    @Override
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        if (!actionsUpdated.hasActionPlanInfo() || !actionsUpdated.hasActionPlanId()) {
            logger.warn("Malformed action update - skipping savings events generation");
            return;
        }
        ActionPlanInfo actionPlanInfo = actionsUpdated.getActionPlanInfo();
        if (!actionPlanInfo.hasMarket()) {
            // We currently only want to see market (vs. buy RI) action plans.
            return;
        }
        logger.debug("Processing onActionsUpdated, actionPlanId = {}",
                actionsUpdated.getActionPlanId());
        TopologyInfo info = actionPlanInfo.getMarket().getSourceTopologyInfo();
        if (TopologyType.REALTIME != info.getTopologyType()) {
            // We only care about real-time actions.
            return;
        }

        logger.debug("Handling realtime action updates");
        /*
         *  - Iterate over actions list and identify resize recommendations.
         *  - Add any target entities that are not currently in the internal state database.
         *  - Insert recommendation events into the event log.
         */
        final Long topologyContextId = info.getTopologyContextId();
        Map<ActionSpec, Long> newPendingActionSpecsToEntityId = new HashMap<>();
        Map<Long, EntityActionInfo> newPendingActionsEntityIdToActionInfo = new HashMap<>();
        AtomicReference<String> cursor = new AtomicReference<>("0");
        do {
            final FilteredActionRequest filteredActionRequest =
                    filteredActionRequest(topologyContextId, cursor);
            actionsService.getAllActions(filteredActionRequest)
                    .forEachRemaining(filteredActionResponse -> {
                        if (filteredActionResponse.hasActionChunk()) {
                            for (ActionOrchestratorAction action
                                    : filteredActionResponse.getActionChunk().getActionsList()) {
                                ActionSpec actionSpec = action.getActionSpec();
                                if (!actionSpec.hasRecommendation()) {
                                    continue;
                                }
                                final Long actionId = actionSpec.getRecommendation().getId();
                                ActionEntity entity;
                                try {
                                    entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
                                } catch (UnsupportedActionException e) {
                                    logger.warn("Cannot create action Savings event due to"
                                        + " unsupported action type for action {}", actionId, e);
                                    continue;
                                }
                                if (!pendingWorkloadTypes.contains(entity.getType())) {
                                    continue;
                                }
                                final Long entityId = entity.getId();
                                logger.debug("Processing savings for action {}, entity {}", actionId, entityId);
                                // Since we're looping through actions here, it's more  efficient to maintain this local
                                // mapping and reverse mapping for O(1) lookup, rather than searching through
                                // one data structure during events creation or retrieval of costs.
                                newPendingActionSpecsToEntityId.put(actionSpec, entityId);
                                newPendingActionsEntityIdToActionInfo.put(entityId,
                                        getEntityActionInfo(actionId, actionSpec, entity));
                                logger.debug("Adding Pending Action {}, at {} for entity {}", actionId,
                                        actionSpec.getRecommendationTime(), entityId);
                            }
                        } else if (filteredActionResponse.hasPaginationResponse()) {
                            cursor.set(filteredActionResponse.getPaginationResponse().getNextCursor());
                        }
                    });
        } while (!StringUtils.isEmpty(cursor.get()));

        // TODO: The way we're processing update and success, we may not need synchronization.
        // However this may need to be re-evaluated in the future, because of potential race conditions,
        // in the presence of multiple threads.

        // Add new pending action events.
        // Compare the old and new actions and create SavingsEvents for new ActionId's.
        // entriesOnlyOnLeft() returns newly added actions, and entriesOnlyOnRight()
        // returns actions no longer being generated by market.
        MapDifference<ActionSpec, Long> actionChanges = Maps.difference(newPendingActionSpecsToEntityId,
                currentPendingActionSpecsToEntityId);
        Map<Long, EntityPriceChange> entityPriceChangeMap = getEntityCosts(
                newPendingActionsEntityIdToActionInfo);
        Set<SavingsEvent> newPendingActionEvents = new HashSet<>();
        actionChanges.entriesOnlyOnLeft().keySet().forEach(newActionSpec -> {
            final Long newActionId = newActionSpec.getRecommendation().getId();
            final ActionState actionState = newActionSpec.getActionState();
            final EntityPriceChange actionPriceChange = entityPriceChangeMap.get(newActionId);
            if (actionPriceChange != null) {
                logger.trace("New action price change for {}, {}, {}, {}:",
                        newActionSpec,
                        actionState, actionPriceChange.getSourceCost(),
                        actionPriceChange.getDestinationCost());
                ActionState prevActionState = entityActionStateMap.get(newActionId);
                if (prevActionState != ActionState.SUCCEEDED) {
                    entityActionStateMap.put(newActionId, actionState);
                    final Long entityId = newPendingActionSpecsToEntityId
                            .get(newActionSpec);
                    SavingsEvent pendingActionEvent =
                            createActionEvent(entityId,
                                    newActionSpec.getRecommendationTime(),
                                    ActionEventType.RECOMMENDATION_ADDED,
                                    newActionId,
                                    actionPriceChange);
                    newPendingActionEvents.add(pendingActionEvent);
                    logger.debug("Added new pending event for action {}, entity {},"
                                    + " action state {}",
                            newActionId, entityId, actionState);
                }
            }
        });
        entityEventsJournal.addEvents(newPendingActionEvents);


        // Add events related to stale pending actions.
        if (!currentPendingActionSpecsToEntityId.isEmpty()) {
            Set<SavingsEvent> staleActionEvents = new HashSet<>();
            actionChanges.entriesOnlyOnRight().keySet().forEach(staleActionSpec -> {
                final Long staleActionId = staleActionSpec.getRecommendation().getId();
                final Long entityId = currentPendingActionSpecsToEntityId
                        .get(staleActionSpec);
                SavingsEvent pendingActionEvent =
                        createActionEvent(entityId,
                                staleActionSpec.getRecommendationTime(),
                                ActionEventType.RECOMMENDATION_REMOVED,
                                staleActionId,
                                emptyPriceChange);
                staleActionEvents.add(pendingActionEvent);
                logger.debug("Added stale event for {}, entity {}", staleActionId, entityId);
                entityActionStateMap.remove(staleActionId);
            });
            entityEventsJournal.addEvents(staleActionEvents);

            // Clear the old and save the latest set of new pending actions.
            currentPendingActionSpecsToEntityId.clear();
        }
        currentPendingActionSpecsToEntityId.putAll(newPendingActionSpecsToEntityId);
    }

    /**
     * Return a request to fetch filtered set of market actions.
     *
     * @param topologyContextId The topology context id of the Market.
     * @param cursor current page to request
     * @return The FilteredActionRequest.
     */
    private FilteredActionRequest filteredActionRequest(final Long topologyContextId,
            AtomicReference<String> cursor) {
        return FilteredActionRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setPaginationParams(PaginationParameters.newBuilder()
                                        .setCursor(cursor.getAndSet("")))
                        .addActionQuery(ActionQuery.newBuilder().setQueryFilter(
                                        ActionQueryFilter
                                            .newBuilder()
                                            .setVisible(true)
                                            .addAllTypes(pendingActionTypes)
                                            .addAllStates(pendingActionStates)
                                            .addAllModes(pendingActionModes)
                                            .setEnvironmentType(EnvironmentType.CLOUD))
                                        .build())
                        .build();
    }

    /**
     * Create a Savings Event on receiving an Action Event.
     *
     * @param entityId The target entity ID.
     * @param timestamp The time-stamp of the action event (execution completion time,
     *                  recommendation tie etc).
     * @param actionType The action type.
     * @param actionId the action ID.
     * @param priceChange the price change associated with the action.
     * @return The SavingsEvent.
     */
    private static SavingsEvent createActionEvent(Long entityId, Long timestamp, ActionEventType actionType,
                              long actionId, @Nonnull final EntityPriceChange priceChange) {
        return new SavingsEvent.Builder()
                        .actionEvent(new ActionEvent.Builder()
                                        .actionId(actionId)
                                        .eventType(actionType).build())
                        .entityId(entityId)
                        .timestamp(timestamp)
                        .entityPriceChange(priceChange)
                        .build();
    }

    /**
     * Getter for entityActionStateMap.
     *
     * @return entityActionStateMap;
     */
    @VisibleForTesting
    protected Map<Long, ActionState> getEntityActionStateMap() {
        return entityActionStateMap;
    }

    /**
     * Getter for currentPendingActionsActionSpecToEntityIdMap.
     *
     * @return currentPendingActionsActionSpecToEntityIdMap.
     */
    @VisibleForTesting
    protected Map<ActionSpec, Long> getCurrentPendingActionsActionSpecToEntityIdMap() {
        return currentPendingActionSpecsToEntityId;
    }


    /**
     * Get applicable (compute, license, storage etc.) rates for a list of target entities.
     *
     * @param entityIdToActionInfoMap Map of entity oid to EntityActionInfo containing action details.
     * @return Map of actionId to EntityPriceChange.
     */
    private Map<Long, EntityPriceChange> getEntityCosts(
            @Nonnull final Map<Long, EntityActionInfo> entityIdToActionInfoMap) {
        Map<Long, EntityPriceChange> actionIdToEntityPriceChange = new HashMap<>();

        // Get subset of entity ids by cost category, we can only make 1 request for each category.
        Map<CostCategory, Set<Long>> categoryToEntities = new HashMap<>();
        entityIdToActionInfoMap.forEach((entityId, entityActionInfo) -> {
            Set<CostCategory> cat = costCategoriesByEntityType.get(entityActionInfo.entityType);
            if (CollectionUtils.isNotEmpty(cat)) {
                cat.forEach(eachCategory -> {
                    categoryToEntities.computeIfAbsent(eachCategory, k -> new HashSet<>()).add(entityId);
                });
            }
        });

        // Make the Grpc call to get before and after costs for those categories.
        queryEntityCosts(categoryToEntities, entityIdToActionInfoMap);

        // Update total costs in return map.
        entityIdToActionInfoMap.forEach((entityId, entityActionInfo) -> {
            final EntityActionCosts totalCosts = entityActionInfo.getTotalCosts();
            final EntityPriceChange actionPriceChange = new EntityPriceChange.Builder()
                    .sourceOid(entityActionInfo.sourceOid)
                    .sourceCost(totalCosts.beforeCosts)
                    .destinationOid(entityActionInfo.destinationOid)
                    .destinationCost(totalCosts.afterCosts)
                    .build();
            actionIdToEntityPriceChange.put(entityActionInfo.actionId, actionPriceChange);
        });
        return actionIdToEntityPriceChange;
    }

    /**
     * Makes the CostRpc calls to get the before and after costs for the given set of entities,
     * for the categories specified.
     *
     * @param categoryToEntities Map of CostCategory to a list of entities for which those costs
     *      need to be fetched. An api call is made per category.
     * @param entityIdToActionInfoMap Input map that is updated with fetched costs.
     */
    @VisibleForTesting
    void queryEntityCosts(@Nonnull final Map<CostCategory, Set<Long>> categoryToEntities,
            @Nonnull final Map<Long, EntityActionInfo> entityIdToActionInfoMap) {
        categoryToEntities.forEach((category, entities) -> {
            final GetTierPriceForEntitiesRequest.Builder request = GetTierPriceForEntitiesRequest
                    .newBuilder()
                    .addAllOids(entities)
                    .setCostCategory(category);
            request.setTopologyContextId(realTimeTopologyContextId);

            final GetTierPriceForEntitiesResponse response = costService.getTierPriceForEntities(
                    request.build());
            final Map<Long, CurrencyAmount> beforeCosts = response.getBeforeTierPriceByEntityOidMap();
            final Map<Long, CurrencyAmount> afterCosts = response.getAfterTierPriceByEntityOidMap();

            entities.forEach(entityId -> {
                final EntityActionInfo entityActionInfo = entityIdToActionInfoMap.get(entityId);
                double beforeAmount = beforeCosts.getOrDefault(entityId, zeroCosts).getAmount();
                double afterAmount = afterCosts.getOrDefault(entityId, zeroCosts).getAmount();
                entityActionInfo.costsByCategory.put(category, new EntityActionCosts(beforeAmount,
                        afterAmount));
            });
        });
    }

    /**
     * Creates a new instance of EntityActionInfo with given input info.
     *
     * @param actionId Id of action.
     * @param actionSpec ActionSpec containing some info like source/destination oid.
     * @param entity ActionEntity instance containing entity type.
     * @return Newly created EntityActionInfo instance.
     */
    @Nonnull
    @VisibleForTesting
    EntityActionInfo getEntityActionInfo(long actionId,
            @Nonnull final ActionSpec actionSpec, @Nonnull final ActionEntity entity) {
        EntityActionInfo entityActionInfo = new EntityActionInfo(actionId);
        final Action action = actionSpec.getRecommendation();
        entityActionInfo.entityType = entity.getType();

        if (action.hasInfo() && action.getInfo().hasScale()) {
            final Scale scale = action.getInfo().getScale();
            if (scale.getChangesCount() > 0) {
                final ChangeProvider changeProvider = action.getInfo().getScale().getChanges(0);
                if (changeProvider.hasSource()) {
                    entityActionInfo.sourceOid = changeProvider.getSource().getId();
                }
                if (changeProvider.hasDestination()) {
                    entityActionInfo.destinationOid = changeProvider.getDestination().getId();
                }
            } else if (scale.hasPrimaryProvider()) {
                // Scaling within same tier, like some UltraSSDs.
                entityActionInfo.sourceOid = scale.getPrimaryProvider().getId();
                entityActionInfo.destinationOid = scale.getPrimaryProvider().getId();
            }
        }
        return entityActionInfo;
    }

    /**
     * Internal use only: Keeps info related to action and cost of an entity. Info here is used
     * to make up the priceChange instance later.
     */
    static class EntityActionInfo {
        /**
         * Type of entity, e.g VM/DB/Volume.
         */
        int entityType;

        /**
         * OID of source (pre-action) tier. 0 if not applicable.
         */
        long sourceOid;

        /**
         * OID of destination (post-action) tier. 0 if not applicable.
         */
        long destinationOid;

        /**
         * Stores before and after costs per category.
         */
        Map<CostCategory, EntityActionCosts> costsByCategory;

        /**
         * Id of action.
         */
        long actionId;

        /**
         * Creates a new instance with the given action id.
         *
         * @param actionId Id of action.
         */
        EntityActionInfo(long actionId) {
            this.actionId = actionId;
            this.sourceOid = 0;
            this.destinationOid = 0;
            this.costsByCategory = new HashMap<>();
        }

        /**
         * Gets summed up costs, across all applicable categories, for this entity.
         *
         * @return Total costs, containing pre and post action costs.
         */
        @Nonnull
        EntityActionCosts getTotalCosts() {
            final EntityActionCosts totalCosts = new EntityActionCosts();
            costsByCategory.values().forEach(totalCosts::add);
            return totalCosts;
        }
    }

    /**
     * Internal use only: For storing either per-category costs or total costs of an entity action.
     * Costs before and after action are stored.
     */
    static class EntityActionCosts {
        /**
         * Costs before action, 'source' costs.
         */
        double beforeCosts;

        /**
         * Costs after action, 'projected' or 'destination' costs.
         */
        double afterCosts;

        /**
         * Create instance with 0 costs.
         */
        EntityActionCosts() {
            this(0d, 0d);
        }

        /**
         * Creates a new instance with specified costs.
         *
         * @param before Costs before.
         * @param after Costs after.
         */
        EntityActionCosts(double before, double after) {
            beforeCosts = before;
            afterCosts = after;
        }

        /**
         * Adds input costs to the costs of this instance. Used to make up total costs from all
         * the individual category costs.
         *
         * @param other Category costs to add to this instance.
         */
        void add(@Nonnull final EntityActionCosts other) {
            beforeCosts += other.beforeCosts;
            afterCosts += other.afterCosts;
        }
    }
}
