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
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
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
     * The Map of current (active/existing) action EntityActionSpec to entityId.
     *
     * <p>This mapping is preserved in order to process stale action events.
     */
    private final Map<EntityActionInfo, Long> existingPendingActionsInfoToEntityId = new ConcurrentHashMap<>();

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
        final Long actionId = actionSuccess.getActionId();
        ActionState prevActionState = entityActionStateMap.get(actionId);
        // Check if a SUCCEEDED action id hasn't already been added, in order to to avoid processing
        // an action more than once if multiple SUCCEEDED notifications were to be received.
        // There could be a previous READY/PENDING_ACCEPT entry, and that's fine.
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
                    final EntityActionInfo entityActionInfo = new EntityActionInfo(actionSpec, entity);
                    final Map<Long, EntityPriceChange> entityPriceChangeMap =
                            getEntityCosts(ImmutableMap.of(entityId, entityActionInfo));
                    final EntityPriceChange actionPriceChange = entityPriceChangeMap.get(actionId);
                    if (actionPriceChange != null) {
                        long expirationTime = completionTime
                                + (ActionEventType.DELETE_EXECUTION_SUCCESS
                                        .equals(entityActionInfo.getActionEventType())
                                                ? deleteVolumeActionLifetimeMs
                                                : actionLifetimeMs);
                        EntityPriceChange actionPriceChangeWithExpiration =
                                new EntityPriceChange.Builder()
                                        .from(actionPriceChange)
                                        .expirationTime(Optional.of(expirationTime))
                                        .build();
                        final SavingsEvent successEvent = createActionEvent(entity.getId(),
                                completionTime,
                                entityActionInfo.getActionEventType(),
                                actionId,
                                actionPriceChangeWithExpiration,
                                entityActionInfo);
                        entityEventsJournal.addEvent(successEvent);
                        entityActionStateMap.put(actionId, ActionState.SUCCEEDED);
                        logger.debug("Added action {} for entity {}, completion time {}, recommendation"
                                        + " time {}, journal size {}",
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
     * <p>Go through all the Market actions in the Market cycle and create the Action Info to Entity Id Mapping.
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
         *  - Iterate over actions list and identify resize(scale) recommendations.
         *  - Add any target entities that are not currently in the internal state database.
         *  - Insert recommendation events into the event log.
         */
        final Long topologyContextId = info.getTopologyContextId();
        // This Map allows comparing the old (saved in currentPendingActionsInfoToEntityId) and new
        // actions and generate appropriate Savings events.  Also it is also used to reverse lookup
        // the fetched on-demand costs of entities by actionId corresponding to an entityId.
        BiMap<EntityActionInfo, Long> newPendingActionsInfoToEntityId = HashBiMap.create();
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
                                logger.debug("Saving Info for Pending Action {}, at {} for entity {}", actionId,
                                             actionSpec.getRecommendationTime(), entityId);
                                newPendingActionsInfoToEntityId.put(new EntityActionInfo(actionSpec, entity), entityId);
                            }
                        } else if (filteredActionResponse.hasPaginationResponse()) {
                            cursor.set(filteredActionResponse.getPaginationResponse().getNextCursor());
                        }
                    });
        } while (!StringUtils.isEmpty(cursor.get()));

        generateRecommendationEvents(newPendingActionsInfoToEntityId);
    }

    /**
     * Generate savings related events for missed savings/investments.
     *
     * <p><p>Process new Pending VM Scale actions and add to RECOMMENDATION_ADDED events to Events Journal.
     * Process stale Pending VM Scale actions and add to RECOMMENDATION_REMOVED events to Events Journal.
     * Retrieve on-demand cost before and after for each action and populate the Savings Events with this information.
     * @param newPendingActionsInfoToEntityId Action Info to Entity Id map of the new set of actions in the Market cycle
     * being processed.
     */
    private void generateRecommendationEvents(@Nonnull final BiMap<EntityActionInfo, Long> newPendingActionsInfoToEntityId) {
        // Add new pending action events.
        // Compare the old and new actions and create SavingsEvents for new ActionId's.
        // entriesOnlyOnLeft() returns newly added actions, and entriesOnlyOnRight()
        // returns actions no longer being generated by or replaced by Market.
        MapDifference<EntityActionInfo, Long> actionChanges = Maps
                        .difference(newPendingActionsInfoToEntityId,
                                    existingPendingActionsInfoToEntityId);
        Map<Long, EntityActionInfo> newPendingEntityIdToActionsInfo =
                                                                    newPendingActionsInfoToEntityId
                                                                                    .inverse();
        Map<Long, EntityPriceChange> entityPriceChangeMap =
                                                  getEntityCosts(newPendingEntityIdToActionsInfo);
        Set<SavingsEvent> newPendingActionEvents = new HashSet<>();
        actionChanges.entriesOnlyOnLeft().keySet().forEach(newActionInfo -> {
            final Long newActionId = newActionInfo.getActionId();
            final ActionState actionState = newActionInfo.getActionState();
            final EntityPriceChange actionPriceChange = entityPriceChangeMap.get(newActionId);
            if (actionPriceChange != null) {
                logger.trace("New action price change for {}, {}, {}, {}:",
                        newActionInfo,
                        actionState, actionPriceChange.getSourceCost(),
                        actionPriceChange.getDestinationCost());
                ActionState prevActionState = entityActionStateMap.get(newActionId);
                if (prevActionState != ActionState.SUCCEEDED) {
                    entityActionStateMap.put(newActionId, actionState);
                    final Long entityId = newPendingActionsInfoToEntityId
                            .get(newActionInfo);
                    SavingsEvent pendingActionEvent =
                            createActionEvent(entityId,
                                    newActionInfo.getRecommendationTime(),
                                    ActionEventType.RECOMMENDATION_ADDED,
                                    newActionId,
                                    actionPriceChange,
                                    newActionInfo);
                    newPendingActionEvents.add(pendingActionEvent);
                    logger.debug("Added new pending event for action {}, entity {},"
                                    + " action state {}, source oid {}, destination oid {}, entity type {}",
                                    newActionId, entityId, actionState, newActionInfo.getSourceOid(),
                                    newActionInfo.getDestinationOid(),
                                    newActionInfo.getEntityType());
                }
            }
        });
        entityEventsJournal.addEvents(newPendingActionEvents);
        // Add events related to stale actions.
        // entityIdsOnLeft represents entities with new actions, or those with a different new action
        // than Market has recommended in previous cycle(s).
        // entityIdsOnRight represents entities with old actions.
        Set<Long> entityIdsOfExistingActions = actionChanges.entriesOnlyOnLeft().values().stream()
                        .collect(Collectors.toSet());
        Set<Long> entityIdsOfNewActions = actionChanges.entriesOnlyOnRight().values().stream()
                        .collect(Collectors.toSet());
        // This intersection of the maps values represent entities that had an action in a previous cycle
        // and have a different (replaced) action in the current cycle.  From this point on missed
        // savings / investment for the entities will accrue based on price change associated with the new action.
        // No stale event is generated for these entities -- as this could cause an issue if the stale event
        // time-stamp were to match the new action time-stamp, and if the stale event were to be processed
        // after the new action, and inadvertently cancel out savings for the new action.
        Set<Long> entitiesWithReplacedActions = Sets.intersection(entityIdsOfExistingActions,
                                                                  entityIdsOfNewActions);
        if (!existingPendingActionsInfoToEntityId.isEmpty()) {
            Set<SavingsEvent> staleActionEvents = new HashSet<>();
            // For actions that are getting removed by Market, make the stale event time-stamp
            // current time, as this will make it higher than the recommendation time of
            // all the actions being removed, as they would have been recommended in a prior cycle.
            final long currentTimeInMillis = System.currentTimeMillis();
            actionChanges.entriesOnlyOnRight().keySet().forEach(staleActionInfo -> {
                final Long staleActionId = staleActionInfo.getActionId();
                final Long entityId = existingPendingActionsInfoToEntityId
                        .get(staleActionInfo);
                if (!entitiesWithReplacedActions.contains(entityId)) {
                    SavingsEvent staleActionEvent =
                                                    createActionEvent(entityId,
                                                                      currentTimeInMillis,
                                                      ActionEventType.RECOMMENDATION_REMOVED,
                                                      staleActionId,
                                                      null,
                                                            staleActionInfo);
                    staleActionEvents.add(staleActionEvent);
                    logger.debug("Added stale event for action {}, entity {},"
                                 + "  source oid {}, destination oid {}, entity type {}",
                                 staleActionId, entityId, staleActionInfo.getSourceOid(),
                                 staleActionInfo.getDestinationOid(),
                                 staleActionInfo.getEntityType());
                    entityActionStateMap.remove(staleActionId);
                } else {
                    logger.debug("Entity {} with old action {} has a different action {} this cycle."
                                    + " No stale event generated",
                                 entityId, staleActionId);
                }
            });
            entityEventsJournal.addEvents(staleActionEvents);

            // Clear the old and save the latest set of new pending actions.
            existingPendingActionsInfoToEntityId.clear();
        }
        existingPendingActionsInfoToEntityId.putAll(newPendingActionsInfoToEntityId);
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
     * @param actionInfo Additional info about action.
     * @return The SavingsEvent.
     */
    private static SavingsEvent createActionEvent(Long entityId, Long timestamp, ActionEventType actionType,
                              long actionId, @Nullable final EntityPriceChange priceChange,
            final EntityActionInfo actionInfo) {
        final SavingsEvent.Builder builder = new SavingsEvent.Builder()
                        .actionEvent(new ActionEvent.Builder()
                                        .actionId(actionId)
                                        .eventType(actionType)
                                .description(actionInfo.getDescription())
                                .entityType(actionInfo.entityType)
                                .actionType(actionInfo.actionType.getNumber())
                                .actionCategory(actionInfo.actionCategory.getNumber())
                                .build())
                        .entityId(entityId)
                        .timestamp(timestamp);
        if (priceChange != null) {
            builder.entityPriceChange(priceChange);
        }
        return builder.build();
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
    protected Map<EntityActionInfo, Long> getCurrentPendingActionsActionSpecToEntityIdMap() {
        return existingPendingActionsInfoToEntityId;
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
            actionIdToEntityPriceChange.put(entityActionInfo.getActionId(), actionPriceChange);
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
     * Internal use only: Keeps info related to action and cost of an entity. Info here is used
     * to make up the priceChange instance later.
     */
    static final class EntityActionInfo {
        /**
         * Type of entity, e.g VM/DB/Volume.
         */
        private final int entityType;

        /**
         * OID of source (pre-action) tier. 0 if not applicable.
         */
        private long sourceOid = 0;

        /**
         * OID of destination (post-action) tier. 0 if not applicable.
         */
        private long destinationOid = 0;

        /**
         * Current ActionState of the Action.
         *
         * <p>This field should not be used for comparison in the equals and
         * hashCode() methods as it can change.
         */
        private final ActionState actionState;

        /**
         * RecommendationTime of the original action.
         */
        private final long recommendationTime;

        /**
         * Stores before and after costs per category.
         */
        private final Map<CostCategory, EntityActionCosts> costsByCategory;

        /**
         * Id of action.
         */
        private final long actionId;

        /**
         * The action type.
         */
        private ActionEventType actionEventType;

        /**
         * Action description text.
         */
        private String description;

        /**
         * Optional additional info for some actions, e.g scale compliance.
         */
        @Nullable
        private String explanation;

        /**
         * Type of action - SCALE or DELETE.
         */
        private ActionType actionType;

        /**
         * Performance or Efficiency category.
         */
        private ActionCategory actionCategory;

        /**
         * Saving/hr that is set in action. Only set (can be 0.0) if it is present, some action
         * types like Reconfigure will not have this value set. Here mainly for logging/description.
         */
        @Nullable
        private Double savingsPerHour;

        /**
         * For trimming redundant text from action description, what to replace.
         */
        private static final String[] descriptionToReplace = new String[] {
                "Scale Virtual Machine ",
                "Scale Database ",
                "Scale Database Server ",
                "Scale Volume ",
                "Delete Unattached ",
                "Scale ",
                "Auto Scaling Groups: "
        };

        /**
         * What to replace with.
         */
        private static final String[] descriptionReplaceWith = new String[] {
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY,
                StringUtils.EMPTY
        };

        /**
         * Constructor.
         *
         * @param actionSpec ActionSpec of an action.
         * @param entity The target entity of the action.
         */
        EntityActionInfo(@Nonnull final ActionSpec actionSpec, @Nonnull final ActionEntity entity) {
            final Action action = actionSpec.getRecommendation();
            this.actionId = action.getId();
            this.recommendationTime = actionSpec.getRecommendationTime();
            this.entityType = entity.getType();
            this.actionState = actionSpec.getActionState();
            this.costsByCategory = new HashMap<>();
            if (!action.hasInfo()) {
                return;
            }
            ActionInfo actionInfo = action.getInfo();
            if (actionInfo.hasScale()) {
                this.actionEventType = ActionEventType.SCALE_EXECUTION_SUCCESS;
                final Scale scale = actionInfo.getScale();
                if (scale.getChangesCount() > 0) {
                    final ChangeProvider changeProvider = scale.getChanges(0);
                    if (changeProvider.hasSource()) {
                        this.sourceOid = changeProvider.getSource().getId();
                    }
                    if (changeProvider.hasDestination()) {
                        this.destinationOid = changeProvider.getDestination().getId();
                    }
                } else if (scale.hasPrimaryProvider()) {
                    // Scaling within same tier, like some UltraSSDs.
                    this.sourceOid = scale.getPrimaryProvider().getId();
                    this.destinationOid = scale.getPrimaryProvider().getId();
                }
            } else if (actionInfo.hasDelete()) {
                this.actionEventType = ActionEventType.DELETE_EXECUTION_SUCCESS;
                final Delete delete = actionInfo.getDelete();
                if (delete.hasSource()) {
                    // A delete is modeled as a resize to zero, so ensure that the destination
                    // OID is zero, which will map to a zero cost.
                    this.sourceOid = delete.getSource().getId();
                    this.destinationOid = 0L;
                }
            }
            this.description = actionSpec.getDescription();
            this.actionCategory = actionSpec.getCategory();
            if (action.hasSavingsPerHour() && action.getSavingsPerHour().hasAmount()) {
                this.savingsPerHour = action.getSavingsPerHour().getAmount();
            }
            processActionType(actionSpec);
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

        /**
         * Getter for entityType.
         *
         * @return entityType.
         */
        protected int getEntityType() {
            return entityType;
        }

        /**
         * Getter for sourceOid.
         *
         * @return sourceOid.
         */
        protected long getSourceOid() {
            return sourceOid;
        }

        /**
         * Getter for destinationOid.
         *
         * @return destinationOid.
         */
        protected long getDestinationOid() {
            return destinationOid;
        }

        /**
         * Getter for costsByCategory.
         *
         * @return costsByCategory.
         */
        protected Map<CostCategory, EntityActionCosts> getCostsByCategory() {
            return costsByCategory;
        }

        /**
         * Getter for actionId.
         *
         * @return actionId.
         */
        protected long getActionId() {
            return actionId;
        }

        /**
         * Getter for actionState.
         *
         * @return actionState.
         */
        protected ActionState getActionState() {
            return actionState;
        }

        /**
         * Getter for recommendationTime.
         *
         * @return recommendationTime.
         */
        protected long getRecommendationTime() {
            return recommendationTime;
        }

        /**
         * Getter for action type.
         *
         * @return the action type.
         */
        protected ActionEventType getActionEventType() {
            return actionEventType;
        }

        /**
         * hashCode() method.
         *
         * <p>This method is used as a map key.  Please don't add ActionSpec fields
         * like _deprecated_importance, savings_per_hour, recommendation_time, actionState, costs etc
         * to check for equality as these may change. As of now using only actionId, sourceOid,
         * destinationOid and entityType as the parameters for equality as we expect them to be constant
         * for a certain action.  If any of these fields were to change, it will be treated as a new action.
         */
        @Override
        public int hashCode() {
            return Objects.hash(actionId, sourceOid, destinationOid, entityType);
        }

        /**
         * equals() method.
         *
         * <p>This method is used as a map key.  Please don't add ActionSpec fields
         * like _deprecated_importance, savings_per_hour, recommendation_time, actionState, costs etc
         * to check for equality as these may change. As of now using only actionId, sourceOid,
         * destinationOid and entityType as the parameters for equality as we expect them to be constant
         * for a certain action.  If any of these fields were to change, it will be treated as a new action.
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            EntityActionInfo other = (EntityActionInfo)obj;
            if (actionId != other.actionId) {
                return false;
            }
            if (destinationOid != other.destinationOid) {
                return false;
            }
            if (entityType != other.entityType) {
                return false;
            }
            if (sourceOid != other.sourceOid) {
                return false;
            }
            return true;
        }

        /**
         * Method to extract Action type from ActionSpec. Would have been nice if spec had a type field!
         * For scale action type, also tries to set the provider oid details.
         *
         * @param actionSpec ActionSpec to check.
         */
        private void processActionType(@Nonnull final ActionSpec actionSpec) {
            this.actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());

            if (this.actionType == ActionType.SCALE
                    && actionSpec.getRecommendation().getInfo().hasScale()) {
                processProviderDetails(actionSpec.getRecommendation().getInfo().getScale());
                if (actionSpec.hasExplanation()) {
                    String exp = actionSpec.getExplanation();
                    if (StringUtils.isNotBlank(exp)) {
                        this.explanation = exp.replace("(^_^)~", "");
                    }
                }
            }
        }

        /**
         * For scale actions, sets the details of source and destination provider.
         *
         * @param scale Scale action info.
         */
        private void processProviderDetails(@Nonnull final Scale scale) {
            if (scale.getChangesCount() > 0) {
                final ChangeProvider changeProvider = scale.getChanges(0);
                if (changeProvider.hasSource()) {
                    this.sourceOid = changeProvider.getSource().getId();
                }
                if (changeProvider.hasDestination()) {
                    this.destinationOid = changeProvider.getDestination().getId();
                }
            } else if (scale.hasPrimaryProvider()) {
                // Scaling within same tier, like some UltraSSDs.
                this.sourceOid = scale.getPrimaryProvider().getId();
                this.destinationOid = scale.getPrimaryProvider().getId();
            }
        }

        /**
         * Gets the description to use in action event to be added to journal.
         *
         * @return Description, plus optionally savings/hr and/or explanation.
         */
        @Nonnull
        String getDescription() {
            final StringBuilder sb = new StringBuilder();
            sb.append(description);
            if (savingsPerHour != null) {
                sb.append(", sph: ").append(savingsPerHour);
            }
            if (StringUtils.isNotBlank(explanation)) {
                sb.append(", exp: ").append(explanation);
            }
            String title = sb.toString();
            return StringUtils.replaceEach(title, descriptionToReplace, descriptionReplaceWith);
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
