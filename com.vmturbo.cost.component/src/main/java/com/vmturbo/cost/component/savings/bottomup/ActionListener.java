package com.vmturbo.cost.component.savings.bottomup;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

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
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.InMemoryEntityCostStore;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.EntityState;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

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
     * The In Memory Events Journal.
     */
    private final EntityEventsJournal entityEventsJournal;

    /**
     * The Map of current (active/existing) action EntityActionSpec to entityId.
     *
     * <p>This mapping is preserved in order to process stale action events.
     */
    private final Map<EntityActionInfo, Long> existingActionsInfoToEntityId = new ConcurrentHashMap<>();

    /**
     * For making Grpc calls to Action Orchestrator.
     */
    private final ActionsServiceBlockingStub actionsService;

    /**
     * Real-time context id.
     */
    private final Long realTimeTopologyContextId;

    /**
     * Pending Action Types.
     */
    private final Set<ActionType> supportedActionTypes;

    /**
     * Pending Action MODES. (Maybe we need to check if executable ? )
     */
    private final ImmutableSet<ActionMode> supportedActionModes = ImmutableSet.of(ActionMode.MANUAL,
            ActionMode.RECOMMEND,
            ActionMode.AUTOMATIC,
            ActionMode.EXTERNAL_APPROVAL);
    /**
     * Pending Action Entity Types.
     */
    private final Set<Integer> supportedWorkloadTypes;

    /**
     * Executable Action Types supported.
     */
    private final Set<ActionEvent.ActionEventType> supportedExecutableActionEventTypes = ImmutableSet
                    .of(ActionEvent.ActionEventType.DELETE_EXECUTION_SUCCESS,
                        ActionEvent.ActionEventType.SCALE_EXECUTION_SUCCESS);
    /**
     * The current entity costs store.
     */
    private final EntityCostStore currentEntityCostStore;

    /**
     * The projected entity costs store.
     */
    private final InMemoryEntityCostStore projectedEntityCostStore;

    /**
     * Map of entity type to a set of cost categories for which costs need to be queried for.
     * The current entity costs store.
     */
    private static final Map<Integer, Set<CostCategory>> costCategoriesByEntityType = new HashMap<>();

    /**
     * Set of Cost Sources for which costs are queried.
     */
    private static final ImmutableSet<CostSource> costSources = ImmutableSet.of(
            CostSource.ON_DEMAND_RATE,
            CostSource.RI_INVENTORY_DISCOUNT);

    /**
     * String to describe before action costs for an entity.
     */
    private final String beforeCosts = "Before Costs";
    /**
     * String to describe after action costs for an entity.
     */
    private final String afterCosts = "After Costs";

    static {
        costCategoriesByEntityType.put(EntityType.VIRTUAL_MACHINE_VALUE,
                                       ImmutableSet.of(CostCategory.ON_DEMAND_COMPUTE,
                                                       CostCategory.ON_DEMAND_LICENSE,
                                                       CostCategory.RESERVED_LICENSE));
        costCategoriesByEntityType.put(EntityType.VIRTUAL_VOLUME_VALUE,
                                       ImmutableSet.of(CostCategory.STORAGE));
        costCategoriesByEntityType.put(EntityType.DATABASE_VALUE,
                                       ImmutableSet.of(CostCategory.ON_DEMAND_COMPUTE,
                                                       CostCategory.STORAGE));
        costCategoriesByEntityType.put(EntityType.DATABASE_SERVER_VALUE,
                                       ImmutableSet.of(CostCategory.ON_DEMAND_COMPUTE,
                                                       CostCategory.STORAGE));
    }

    /**
     * Action lifetimes.
     */
    private final EntitySavingsRetentionConfig retentionConfig;

    private final Clock clock;

    /**
     * Entity Savings Store.
     */
    private final EntitySavingsStore<DSLContext> entitySavingsStore;

    /**
     * Entity Savings rollup times store.
     */
    private final RollupTimesStore entitySavingsRollupTimesStore;

    /**
     * Entity State Store.
     */
    private final EntityStateStore<DSLContext> entityStateStore;

    /**
     * This flag indicates whether we have recovered the events the action listener has missed
     * before the cost component was restarted.
     */
    private boolean actionsCaughtUp = false;

    /**
     * Constructor.
     *
     * @param entityEventsInMemoryJournal Entity Events Journal to maintain Savings events including those related to actions.
     * @param actionsServiceBlockingStub Stub for Grpc calls to actions service.
     * @param costStoreHouse Entity cost store
     * @param projectedEntityCostStore Projected entity cost store
     * @param realTimeContextId The real-time topology context id.
     * @param supportedEntityTypes Set of entity types supported.
     * @param supportedActionTypes Set of action types supported.
     * @param retentionConfig savings action retention configuration.
     * @param entitySavingsStore entity savings store
     * @param entityStateStore entity state store
     * @param entitySavingsRollupTimesStore Entity savings rollup times store.
     * @param clock clock
     */
    public ActionListener(@Nonnull final EntityEventsJournal entityEventsInMemoryJournal,
            @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
            @Nonnull final EntityCostStore costStoreHouse,
            @Nonnull final InMemoryEntityCostStore projectedEntityCostStore,
            @Nonnull final Long realTimeContextId,
            @Nonnull Set<EntityType> supportedEntityTypes,
            @Nonnull Set<ActionType> supportedActionTypes,
            @Nonnull EntitySavingsRetentionConfig retentionConfig,
            @Nonnull final EntitySavingsStore<DSLContext> entitySavingsStore,
            @Nonnull final EntityStateStore<DSLContext> entityStateStore,
            @Nonnull final RollupTimesStore entitySavingsRollupTimesStore,
            @Nonnull final Clock clock) {
        this.entityEventsJournal = Objects.requireNonNull(entityEventsInMemoryJournal);
        this.actionsService = Objects.requireNonNull(actionsServiceBlockingStub);
        this.currentEntityCostStore = Objects.requireNonNull(costStoreHouse);
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.realTimeTopologyContextId = realTimeContextId;
        this.supportedWorkloadTypes = supportedEntityTypes.stream()
                .map(EntityType::getNumber)
                .collect(Collectors.toSet());
        this.supportedActionTypes = supportedActionTypes;
        this.retentionConfig = retentionConfig;
        this.entitySavingsStore = entitySavingsStore;
        this.entityStateStore = entityStateStore;
        this.entitySavingsRollupTimesStore = entitySavingsRollupTimesStore;
        this.clock = clock;
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
        // Refresh action expiration settings
        retentionConfig.updateValues();
        // Locate the target entity in the internal entity state.  If not present, create an
        //  - entry for it.
        final Long actionId = actionSuccess.getActionId();
        logger.debug("Got success notification for action {}", actionId);
        final ActionSpec actionSpec = actionSuccess.getActionSpec();

        try {
            // Filter out on-prem actions.  We only handle savings from cloud actions.
            ActionEntity entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
            if (!entity.hasEnvironmentType()
                || entity.getEnvironmentType() != EnvironmentType.CLOUD) {
                logger.debug("Dropping {}  successfully executed action {}", entity.getEnvironmentType(),
                            actionId);
                return;
            }
            createActionSuccessEvent(actionId, actionSpec, entity);
        } catch (UnsupportedActionException e) {
            logger.error("Cannot create action Savings event due to unsupported action type for action {}",
                    actionId, e);
        }
    }

    private void createActionSuccessEvent(Long actionId, ActionSpec actionSpec, ActionEntity entity) {
        final ActionState actionState = actionSpec.getActionState();
        logger.info("Action State of Action {} changed to {}", actionId, actionState);
        // Multiple SUCCEEDED notifications could likely still be received from probes,
        // However the Savings Calculator will only process the first Savings event
        // related to action execution and drop the rest.
        final EntityActionInfo entityActionInfo = new EntityActionInfo(actionSpec, entity);
        if (supportedWorkloadTypes.contains(entity.getType())
            && supportedExecutableActionEventTypes.contains(entityActionInfo.getActionEventType())) {
            final Long completionTime = actionSpec.getExecutionStep().getCompletionTime();
            final Long entityId = entity.getId();
            // The Savings Calculator preserves the recommendation prices, hence executions events don't
            // need to have a EntityPriceChange with before and after costs populated.
            long expirationTime = completionTime
                    + (ActionEvent.ActionEventType.DELETE_EXECUTION_SUCCESS
                    .equals(entityActionInfo.getActionEventType())
                    ? retentionConfig.getVolumeDeleteRetentionMs()
                    : retentionConfig.getActionRetentionMs());
            EntityPriceChange entityPriceChange = new EntityPriceChange.Builder()
                    .sourceCost(0d)       // These are not used. The costs in the recommendation are
                    .destinationCost(0d)  //   used instead.
                    .sourceOid(entityActionInfo.getSourceOid())
                    .destinationOid(entityActionInfo.getDestinationOid())
                    .build();
            final SavingsEvent successEvent = createActionEvent(entity.getId(),
                    completionTime,
                    entityActionInfo.getActionEventType(),
                    actionId, entityPriceChange,
                    entityActionInfo, Optional.of(expirationTime));
            entityEventsJournal.addEvent(successEvent);
            logger.debug("Added action {} for entity {}, completion time {}, recommendation"
                            + " time {}, journal size {}",
                    actionId, entityId, completionTime,
                    actionSpec.getRecommendationTime(),
                    entityEventsJournal.size());
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
        // This Map allows comparing the old (saved in existingActionsInfoToEntityId) and new
        // actions and generate appropriate Savings events.  Also it is also used to reverse lookup
        // the fetched on-demand costs of entities by actionId corresponding to an entityId.
        BiMap<EntityActionInfo, Long> newActionsInfoToEntityId = HashBiMap.create();
        AtomicReference<String> cursor = new AtomicReference<>("0");
        do {
            // This request for actions will not filter by time range or action states.
            // Actions with SUCCEEDED state is included so that if a RECOMMENDATION_ADDED event will
            // be created for the action even if we did not see its earlier states.
            final FilteredActionRequest filteredActionRequest =
                    filteredActionRequest(topologyContextId, cursor, null, null, null);
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
                                if (!supportedWorkloadTypes.contains(entity.getType())) {
                                    continue;
                                }
                                final Long entityId = entity.getId();
                                logger.debug("Saving Info for Action {}, of type {}, at {}"
                                                        + " for entity {}", actionId,
                                                     ActionDTOUtil.getActionInfoActionType(
                                                                   actionSpec.getRecommendation()),
                                                     actionSpec.getRecommendationTime(), entityId);
                                try {
                                    final EntityActionInfo eai = new EntityActionInfo(actionSpec, entity);
                                    newActionsInfoToEntityId.put(eai, entityId);
                                } catch (IllegalArgumentException e) {
                                     logger.warn("Discarding action {} because the entity {} already has an action associated with it. "
                                                     + "An entity cannot have more than one action at the moment. "
                                                     + "This could be a duplicate action for a multi-attach volume. {}",
                                                        actionId, entityId, e.getMessage());
                                }
                            }
                        } else if (filteredActionResponse.hasPaginationResponse()) {
                            cursor.set(filteredActionResponse.getPaginationResponse().getNextCursor());
                        }
                    });
        } while (!StringUtils.isEmpty(cursor.get()));

        generateRecommendationEvents(newActionsInfoToEntityId);
    }

    /**
     * Handle action state transitions.  We currently do nothing other than log the transition.
     * These transitions are currently logged at the info level while the feature is hidden behind
     * a feature flag and is in private preview.  This should be moved to debug level once the
     * feature is permanently enabled.
     *
     * @param actionProgress action progress information.
     */
    @Override
    public void onActionProgress(@Nonnull ActionProgress actionProgress) {
        logger.info("Action ID {} progress {}%: {}",
                actionProgress.getActionId(),
                actionProgress.getProgressPercentage(),
                actionProgress.getDescription());
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
        final long currentTime = clock.millis();
        final long periodStartTime = getNextPeriodStartTime();

        // Attempt recovering action events if this is the first time action listener is run after
        // the cost pod starts up and the entity savings stats tables are not empty (i.e. not the
        // first time the feature is enabled.
        if (!FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()) {
            if (!actionsCaughtUp && periodStartTime != 0) {
                recoverActions(newPendingActionsInfoToEntityId.values(), periodStartTime,
                        currentTime);
            }
        }

        Map<Long, EntityActionInfo> newPendingEntityIdToActionsInfo =
                                                                    newPendingActionsInfoToEntityId
                                                                                    .inverse();
        // Query for action costs and update the action info of the new cycle actions.
        Map<Long, EntityPriceChange> entityPriceChangeMap =
                                                  getEntityCosts(newPendingEntityIdToActionsInfo);

        Set<SavingsEvent> newPendingActionEvents = new HashSet<>();
        // Add new recommendation action events.
        // Compare the old and new actions and create SavingsEvents for new ActionId's.
        // entriesOnlyOnLeft() returns newly added actions, and entriesOnlyOnRight()
        // returns actions no longer being generated by or replaced by Market.
        MapDifference<EntityActionInfo, Long> actionChanges = Maps
                        .difference(newPendingActionsInfoToEntityId,
                                    existingActionsInfoToEntityId);
        actionChanges.entriesOnlyOnLeft().forEach((newActionInfo, entityId) -> {
            final Long newActionId = newActionInfo.getActionId();
            final ActionState actionState = newActionInfo.getActionState();
            final EntityPriceChange actionPriceChange = entityPriceChangeMap.get(newActionId);
            // If the time of the recommendation is before the period start time, we will use the
            // current time. This situation will happen when the action is updated (e.g. a new cost)
            // Using the original recommendation time that is before the period start time will
            // cause the event to be removed.
            long eventTime = newActionInfo.recommendationTime < periodStartTime
                    ? currentTime : newActionInfo.getRecommendationTime();
            boolean recommendationExistsForActionId = existingActionsInfoToEntityId.keySet().stream()
                    .map(EntityActionInfo::getActionId)
                    .anyMatch(id -> id.equals(newActionId));
            // We require all actions to have an RECOMMENDATION_ADDED event, but we want to avoid
            // querying entity costs when an action is in progress. Therefore, we will only generate
            // the RECOMMENDATION_ADDED event if an action is in READY state OR we don't already
            // have a RECOMMENDATION_ADDED event for the action.
            if (actionPriceChange != null
                    && (ActionState.READY.equals(actionState) || !recommendationExistsForActionId)) {
                SavingsEvent pendingActionEvent = createActionEvent(entityId,
                        eventTime,
                        ActionEvent.ActionEventType.RECOMMENDATION_ADDED,
                        newActionId,
                        actionPriceChange,
                        newActionInfo, Optional.empty());
                newPendingActionEvents.add(pendingActionEvent);
                if (logger.isDebugEnabled()) {
                    logger.debug("Created RECOMMENDATION_ADDED event for action {}, action state {}, "
                                    + "entity {}, entity type {}, source OID {}, source cost {}, "
                                    + "destination OID {}, destination cost {}",
                            newActionId, actionState, entityId, newActionInfo.getEntityType(),
                            newActionInfo.getSourceOid(), actionPriceChange.getSourceCost(),
                            newActionInfo.getDestinationOid(), actionPriceChange.getDestinationCost());
                }
            }
        });
        final Stopwatch pendingWatch = Stopwatch.createStarted();
        entityEventsJournal.addEvents(newPendingActionEvents);
        logger.debug("Addition of {} pending recommendation events took {} ms.",
            newPendingActionEvents.size(), pendingWatch.elapsed(TimeUnit.MILLISECONDS));

        // Add events related to stale actions.
        // entityIdsOnLeft represents entities with new actions, or those with a different new action
        // than Market has recommended in previous cycle(s).
        // entityIdsOnRight represents entities with old actions.
        Set<Long> entityIdsOfNewActions = actionChanges.entriesOnlyOnLeft().values().stream()
                        .collect(Collectors.toSet());
        Set<Long> entityIdsOfExistingActions = actionChanges.entriesOnlyOnRight().values().stream()
                        .collect(Collectors.toSet());
        // This intersection of the maps values represent entities that had an action in a previous cycle
        // and have a different (replaced) action in the current cycle.  From this point on missed
        // savings / investment for the entities will accrue based on price change associated with the new action.
        // No stale event is generated for these entities -- as this could cause an issue if the stale event
        // time-stamp were to match the new action time-stamp, and if the stale event were to be processed
        // after the new action, and inadvertently cancel out savings for the new action.
        Set<Long> entitiesWithReplacedActions = Sets.intersection(entityIdsOfExistingActions,
                                                                  entityIdsOfNewActions);
        if (!existingActionsInfoToEntityId.isEmpty()) {
            Set<SavingsEvent> staleActionEvents = new HashSet<>();
            // For actions that are getting removed by Market, make the stale event time-stamp
            // current time, as this will make it higher than the recommendation time of
            // all the actions being removed, as they would have been recommended in a prior cycle.
            actionChanges.entriesOnlyOnRight().forEach((staleActionInfo, entityId) -> {
                final Long staleActionId = staleActionInfo.getActionId();
                if (!entitiesWithReplacedActions.contains(entityId)) {
                    SavingsEvent staleActionEvent =
                                                  createActionEvent(entityId,
                                                            currentTime,
                                                            ActionEvent.ActionEventType.RECOMMENDATION_REMOVED,
                                                            staleActionId,
                                                            null,
                                                            staleActionInfo, Optional.empty());
                    staleActionEvents.add(staleActionEvent);
                    logger.debug("Added stale event for action {}, entity {},"
                                 + "  source oid {}, destination oid {}, entity type {}",
                                 staleActionId, entityId, staleActionInfo.getSourceOid(),
                                 staleActionInfo.getDestinationOid(),
                                 staleActionInfo.getEntityType());
                } else {
                    logger.debug("Entity {} with old action has a different action {} this "
                                    + "cycle. No stale event generated",
                                 entityId, staleActionId);
                }
            });

            if (!staleActionEvents.isEmpty()) {
                final Stopwatch staleWatch = Stopwatch.createStarted();
                entityEventsJournal.addEvents(staleActionEvents);
                logger.debug("Addition of {} stale recommendation events took {} ms.",
                        staleActionEvents.size(), staleWatch.elapsed(TimeUnit.MILLISECONDS));
            }

            // Clear the old and save the latest set of new pending actions.
            existingActionsInfoToEntityId.clear();
        }
        existingActionsInfoToEntityId.putAll(newPendingActionsInfoToEntityId);
    }

    /**
     * Return the time of the next savings processing cycle. It equals to the end time of the
     * most recent processing cycle.
     *
     * @return start time of the next processing cycle, or 0 if the stats table is empty.
     */
    @VisibleForTesting
    long getNextPeriodStartTime() {
        LastRollupTimes lastRollupTimes = entitySavingsRollupTimesStore.getLastRollupTimes();
        if (lastRollupTimes.getLastTimeByHour() == 0) {
            return 0;
        }
        return lastRollupTimes.getLastTimeByHour() + TimeUnit.HOURS.toMillis(1);
    }

    /**
     * Return a request to fetch filtered set of market actions.
     *
     * @param topologyContextId The topology context id of the Market.
     * @param cursor current page to request
     * @param startDate start time in milliseconds
     * @param endDate end time in milliseconds
     * @param actionStates action states
     * @return The FilteredActionRequest.
     */
    @VisibleForTesting
    FilteredActionRequest filteredActionRequest(final Long topologyContextId,
            AtomicReference<String> cursor, @Nullable Long startDate, @Nullable Long endDate,
            @Nullable List<ActionState> actionStates) {
        ActionQueryFilter.Builder actionQueryFilter = ActionQueryFilter
                .newBuilder()
                .setVisible(true)
                .addAllTypes(supportedActionTypes)
                .addAllModes(supportedActionModes)
                .setEnvironmentType(EnvironmentType.CLOUD);
        if (startDate != null && endDate != null) {
            actionQueryFilter.setStartDate(startDate);
            actionQueryFilter.setEndDate(endDate);
        }
        if (actionStates != null) {
            actionQueryFilter.addAllStates(actionStates);
        }
        return FilteredActionRequest.newBuilder()
                        .setTopologyContextId(topologyContextId)
                        .setPaginationParams(PaginationParameters.newBuilder()
                                        .setCursor(cursor.getAndSet("")))
                        .addActionQuery(ActionQuery.newBuilder().setQueryFilter(actionQueryFilter)
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
     * @param expiration Time in milliseconds after execution when the action will expire.
     * @return The SavingsEvent.
     */
    private static SavingsEvent
            createActionEvent(Long entityId, Long timestamp, ActionEvent.ActionEventType actionType,
                              long actionId, final EntityPriceChange priceChange,
                              final EntityActionInfo actionInfo, final Optional<Long> expiration) {
        final ActionEvent.Builder actionEventBuilder = new ActionEvent.Builder()
                        .actionId(actionId)
                        .eventType(actionType)
                        .description(actionInfo.getDescription())
                        .entityType(actionInfo.entityType)
                        .actionType(actionInfo.actionType.getNumber())
                        .actionCategory(actionInfo.actionCategory.getNumber());
        final SavingsEvent.Builder builder = new SavingsEvent.Builder()
                        .actionEvent(actionEventBuilder
                                        .build())
                        .entityId(entityId)
                        .timestamp(timestamp);
        if (priceChange != null) {
            builder.entityPriceChange(priceChange);
        }
        if (expiration.isPresent()) {
            builder.expirationTime(expiration);
        }
        return builder.build();
    }

    /**
     * Getter for existingActionsActionSpecToEntityIdMap.
     *
     * @return existingActionsActionSpecToEntityIdMap.
     */
    @VisibleForTesting
    Map<EntityActionInfo, Long> getExistingActionsInfoToEntityIdMap() {
        return existingActionsInfoToEntityId;
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

        //Get before and after costs for the defined costSources.
        queryEntityCosts(entityIdToActionInfoMap);

        // Update total costs in return map.
        entityIdToActionInfoMap.forEach((entityId, entityActionInfo) -> {
            final EntityActionCosts entityActionCosts = entityActionInfo.getEntityActionCosts();
            final EntityPriceChange actionPriceChange = new EntityPriceChange.Builder()
                            .sourceOid(entityActionInfo.sourceOid)
                            .sourceCost(entityActionCosts.beforeCosts)
                            .destinationOid(entityActionInfo.destinationOid)
                            .destinationCost(entityActionCosts.afterCosts)
                            .build();
            logger.debug("Adding a price change for action {} --> Before Costs {}, After Cost {}",
                         entityActionInfo.getActionId(),
                         entityActionCosts.beforeCosts, entityActionCosts.afterCosts);
            actionIdToEntityPriceChange.put(entityActionInfo.getActionId(), actionPriceChange);
        });
        return actionIdToEntityPriceChange;
    }

    /**
     * Fetches from entityCostStore the before and after costs for the given set of entities,
     * for the CostSources ON_DEMAND_RATE and RI_INVENTORY_DISCOUNT.
     *
     * @param entityIdToActionInfoMap Input map that is updated with fetched costs.
     */
    @VisibleForTesting
    void queryEntityCosts(@Nonnull final Map<Long, EntityActionInfo> entityIdToActionInfoMap) {
        try {
            final Set<CostCategory> costCategories = costCategoriesByEntityType.values()
                                                            .stream()
                                                            .flatMap(x -> x.stream())
                                                            .collect(Collectors.toSet());
            final EntityCostFilter filterBuilder = EntityCostFilterBuilder
                            .newBuilder(TimeFrame.LATEST,
                                        realTimeTopologyContextId)
                            .entityIds(entityIdToActionInfoMap.keySet())
                            .costCategoryFilter(CostCategoryFilter.newBuilder()
                                            .setExclusionFilter(false)
                                            .addAllCostCategory(costCategories)
                                            .build())
                            .latestTimestampRequested(true)
                            .costSources(false, costSources)
                            .build();

            Map<Long, Map<Long, EntityCost>> queryResult =
                                                         currentEntityCostStore
                                                         .getEntityCosts(filterBuilder);
            Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
            queryResult.values().forEach(beforeEntityCostbyOid::putAll);

            final Map<Long, EntityCost> afterEntityCostByOid = projectedEntityCostStore.getEntityCosts(filterBuilder);

            // Populate before costs for entity.
            populateCostsForEntity(beforeEntityCostbyOid, entityIdToActionInfoMap, true);
            // Populate after costs for entity.
            populateCostsForEntity(afterEntityCostByOid, entityIdToActionInfoMap, false);
        } catch (DbException e) {
            logger.warn("ActionListener Error retrieving entity costs", e);
        }
    }

    /**
     * Populate before or after costs for an entity.
     *
     * @param costsMap The entity's cost map.
     * @param entityIdToActionInfoMap  Entity Id to EntityActionInfo {@link EntityActionInfo} map
     * @param isBeforeCosts specifies whether it's the before costs or after posts that need to be populated.
     */
    private void populateCostsForEntity(@Nonnull Map<Long, EntityCost> costsMap,
                                        @Nonnull final Map<Long, EntityActionInfo> entityIdToActionInfoMap,
                                       final boolean isBeforeCosts) {
        int noOfEntitieswithError = 0;
        final String costDescription = isBeforeCosts ? beforeCosts : afterCosts;
        for (Map.Entry<Long, EntityCost> entry : costsMap.entrySet()) {
            final Long entityId = entry.getKey();
            final EntityCost cost = entry.getValue();
            final EntityActionInfo entityActionInfo = entityIdToActionInfoMap.get(entityId);
            if (CollectionUtils.isNotEmpty(cost.getComponentCostList())) {
                AtomicReference<Double> atomicSum = new AtomicReference<>(0.0);
                cost.getComponentCostList().forEach(componentCost -> {
                    // Consider only the Cost Categories relevant for an Entity Type.
                    if (entityIdToActionInfoMap.get(entityId) != null
                            && costCategoriesByEntityType.get(entityIdToActionInfoMap
                            .get(entityId).getEntityType()).contains(componentCost.getCategory())
                        && componentCost.hasAmount()) {
                        logger.debug("Entity {} {} --> CostSource {} : {} : {}",
                                     entityId, costDescription,
                                     componentCost.getCostSource(),
                                     componentCost.getCategory(),
                                     componentCost.getAmount().getAmount());
                        atomicSum.accumulateAndGet(componentCost.getAmount()
                                        .getAmount(), (x, y) -> x + y);
                    }
                });
                final double totalCosts = atomicSum.get();
                if (isBeforeCosts) {
                    logger.debug("Total before costs for entity {} : {}", entityId, totalCosts);
                    entityActionInfo.getEntityActionCosts().setBeforeCosts(totalCosts);
                } else {
                    logger.debug("Total after costs for entity {} : {}", entityId, totalCosts);
                    entityActionInfo.getEntityActionCosts().setAfterCosts(totalCosts);
                }
            } else {
                noOfEntitieswithError++;
                logger.warn("No costs could be retrieved from database for entity having oid {}",
                             entityId);
            }
        }
        if (noOfEntitieswithError > 0) {
            logger.warn("Total number of entities with cost retrieval issues {}.",
                    noOfEntitieswithError);
        }
    }

    private void recoverActions(Set<Long> entitiesWithActionsAfterRestart, long startTime, long currentTime) {
        logger.info("Start recovering actions missed when cost component was down.");
        recoverMissedExecutionSuccessEvents(startTime, currentTime);
        recoverMissedRecommendationRemovedEvents(entitiesWithActionsAfterRestart, currentTime);
        actionsCaughtUp = true;
    }

    /**
     * Recover action succeeded events that happened when cost pod was down. Query the action
     * orchestrator component for actions in the SUCCEEDED state between the time the entity savings
     * was last processed and the current time.
     *
     * @param startTime start time
     * @param endTime end time
     */
    @VisibleForTesting
    void recoverMissedExecutionSuccessEvents(long startTime, long endTime) {
        AtomicReference<String> cursor = new AtomicReference<>("0");
        FilteredActionRequest actionRequest = filteredActionRequest(realTimeTopologyContextId,
                cursor, startTime, endTime, Collections.singletonList(ActionState.SUCCEEDED));
        actionsService.getAllActions(actionRequest)
                .forEachRemaining(actionResponse -> {
                    if (actionResponse.hasActionChunk()) {
                        for (ActionOrchestratorAction action : actionResponse.getActionChunk().getActionsList()) {
                            ActionSpec actionSpec = action.getActionSpec();
                            long actionId = action.getActionId();
                            try {
                                ActionEntity entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
                                createActionSuccessEvent(actionId, actionSpec, entity);
                                logger.info("Execution success event happened when cost component was off. "
                                                + "Adding it back to event journal. Action completion time: {}",
                                        actionSpec.getExecutionStep().getCompletionTime());
                            } catch (UnsupportedActionException e) {
                                logger.error("Cannot create action Savings event due to unsupported action type for action {}",
                                        actionId, e);
                            }
                        }
                    } else if (actionResponse.hasPaginationResponse()) {
                        cursor.set(actionResponse.getPaginationResponse().getNextCursor());
                    }
                });
    }

    /**
     * If an entity has a pending action before the cost pod was down, but the entity no longer has
     * an action after the cost pod restart, we will create a RECOMMENDATION_REMOVED event for this
     * entity.
     *
     * @param entitiesWithActionsAfterRestart entities with action after the restart.
     * @param eventTime The timestamp of the RECOMMENDATION_REMOVED action to be created.
     */
    @VisibleForTesting
    void recoverMissedRecommendationRemovedEvents(Set<Long> entitiesWithActionsAfterRestart,
            Long eventTime) {
        try {
            final Set<Long> entitiesWithActionsBeforeRestart = new HashSet<>();
            entityStateStore.getAllEntityStates(s -> {
                if (s.getCurrentRecommendation() != null && s.getCurrentRecommendation().active()) {
                    entitiesWithActionsBeforeRestart.add(s.getEntityId());
                }
            });
            // Find entities that had an action before but no longer have an action by removing
            // entities of entities that currently have actions from the set of entity IDs of entities
            // that had actions before.
            logger.info("There were {} entities with actions before cost component was down, "
                    + "and there are {} entities with actions now.",
                    entitiesWithActionsBeforeRestart.size(), entitiesWithActionsAfterRestart.size());
            entitiesWithActionsBeforeRestart.removeAll(entitiesWithActionsAfterRestart);
            if (!entitiesWithActionsBeforeRestart.isEmpty()) {
                Map<Long, EntityState> states =
                        entityStateStore.getEntityStates(entitiesWithActionsBeforeRestart);
                final Stopwatch removalWatch = Stopwatch.createStarted();
                states.values().forEach(s -> {
                    // Reconstruct the event. Use dummy values where needed. The algorithm
                    // only needs the time of the event and needs to know it is a
                    // recommendation removed event. The other values are not used by the
                    // algorithm, but will be recorded in the audit log.
                    long dummyActionId = 0L;
                    String actionDescription = "Recommendation was removed after cost restarted.";
                    EntityActionInfo actionInfo = new EntityActionInfo(dummyActionId, eventTime,
                            new EntityActionCosts(), actionDescription, EntityType.UNKNOWN_VALUE,
                            ActionType.SCALE, ActionCategory.EFFICIENCY_IMPROVEMENT);
                    SavingsEvent staleActionEvent =
                            createActionEvent(s.getEntityId(),
                                    eventTime,
                                    ActionEvent.ActionEventType.RECOMMENDATION_REMOVED,
                                    dummyActionId,
                                    null,
                                    actionInfo, Optional.empty());
                    entityEventsJournal.addEvent(staleActionEvent);
                    logger.debug("Action recommendation for entity {} was removed when "
                                    + "cost component was off. Adding it back to event journal.",
                            s.getEntityId());
                });
                logger.info("Addition of {} recommendation removed events took {} ms.",
                        states.values().size(), removalWatch.elapsed(TimeUnit.MILLISECONDS));
            }
        } catch (EntitySavingsException e) {
            logger.error("Error occurred when getting entities states from entity state store.", e);
        }
    }

    /**
     * Internal use only: Keeps info related to action and cost of an entity. Info here is used
     * to make up the priceChange instance later.
     */
    public static final class EntityActionInfo {
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
        private ActionState actionState;

        /**
         * RecommendationTime of the original action.
         */
        private final long recommendationTime;

        /**
         * The before and after costs associated with the action.
         */
        @Nonnull
        private final EntityActionCosts entityActionCosts;

        /**
         * Id of action.
         */
        private final long actionId;

        /**
         * The action type.
         */
        private ActionEvent.ActionEventType actionEventType;

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
            this.entityActionCosts = new EntityActionCosts(0, 0);
            if (!action.hasInfo()) {
                return;
            }
            ActionInfo actionInfo = action.getInfo();
            if (actionInfo.hasScale()) {
                this.actionEventType = ActionEvent.ActionEventType.SCALE_EXECUTION_SUCCESS;
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
                this.actionEventType = ActionEvent.ActionEventType.DELETE_EXECUTION_SUCCESS;
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

        EntityActionInfo(long actionId, long recommendationTime,
                @Nonnull EntityActionCosts entityActionCosts, String description, int entityType,
                ActionType actionType, ActionCategory actionCategory) {
            this.recommendationTime = recommendationTime;
            this.entityActionCosts = entityActionCosts;
            this.entityType = entityType;
            this.actionId = actionId;
            this.description = description;
            this.actionType = actionType;
            this.actionCategory = actionCategory;
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
         * Setter for actionState.
         *
         * @param actionState new action state.
         */
        public void setActionState(ActionState actionState) {
            this.actionState = actionState;
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
        protected ActionEvent.ActionEventType getActionEventType() {
            return actionEventType;
        }

        /**
         *  Getter for EntityActionCosts.
         *
         * @return EntityActionCosts.
         */
        @Nonnull
        protected EntityActionCosts getEntityActionCosts() {
            return entityActionCosts;
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
            return Objects.hash(actionId, sourceOid, destinationOid, entityType, entityActionCosts);
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
            if (!entityActionCosts.equals(other.getEntityActionCosts())) {
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
         * Getter for beforeCosts.
         *
         * @return beforeCosts.
         */
        protected double getBeforeCosts() {
            return beforeCosts;
        }

        /**
         * Setter for beforeCosts.
         *
         * @param beforeCosts The before costs.
         */
        protected void setBeforeCosts(double beforeCosts) {
            this.beforeCosts = beforeCosts;
        }

        /**
         * Getter for afterCosts.
         *
         * @return afterCosts.
         */
        protected double getAfterCosts() {
            return afterCosts;
        }

        /**
         * Setter for afterCosts.
         *
         * @param afterCosts The after costs.
         */
        protected void setAfterCosts(double afterCosts) {
            this.afterCosts = afterCosts;
        }

        /**
         * hashCode() method.
         */
        @Override
        public int hashCode() {
            return Objects.hash(afterCosts, beforeCosts);
        }

        /**
         * equals() method.
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
            EntityActionCosts other = (EntityActionCosts)obj;
            if (Double.compare(afterCosts, other.afterCosts) != 0) {
                return false;
            }
            if (Double.compare(beforeCosts, other.beforeCosts) != 0) {
                return false;
            }
            return true;
        }
    }
}
