package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest.ActionQuery;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
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
    private Map<ActionSpec, Long> currentPendingActionSpecsToEntityId = new ConcurrentHashMap<>();

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
    private Long realTimeTopologyContextId;

    /**
     * Pending Action Types.
     */
    private final ImmutableSet<ActionType> pendingActionTypes = ImmutableSet.of(ActionType.SCALE);

    /**
     * Pending Action MODES. (Maybe we need to check if executable ? )
     */
    private final ImmutableSet<ActionMode> pendingActionModes = ImmutableSet.of(ActionMode.MANUAL,
                                                                                ActionMode.RECOMMEND);
    /**
     * Pending Action States.
     */
    private final ImmutableSet<ActionState> pendingActionStates =
                                                                ImmutableSet.of(ActionState.READY);
    /**
     * Pending Action Entity Types.
     *
     * <p>TODO: Change to TopologyDTOUtil's WORKLOAD_TYPES once code to retrive on-demand costs
     * of storage and DB has been added.
     */
    private final ImmutableSet<Integer> pendingActionWorkloadTypes = ImmutableSet
                    .of(EntityType.VIRTUAL_MACHINE_VALUE);

    /**
     * Constructor.
     *
     * @param entityEventsInMemoryJournal Entity Events Journal to maintain Savings events including those related to actions.
     * @param actionsServiceBlockingStub Stub for Grpc calls to actions service.
     * @param costServiceBlockingStub Stub for Grpc calls to cost service.
     * @param realTimeContextId The real-time topology context id.
     */
    ActionListener(@Nonnull final EntityEventsJournal entityEventsInMemoryJournal,
                    @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                    @Nonnull CostServiceBlockingStub costServiceBlockingStub,
                    @Nonnull final Long realTimeContextId) {
        entityEventsJournal = Objects.requireNonNull(entityEventsInMemoryJournal);
        actionsService = Objects.requireNonNull(actionsServiceBlockingStub);
        costService = Objects.requireNonNull(costServiceBlockingStub);
        realTimeTopologyContextId = realTimeContextId;
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
                entity = ActionDTOUtil.getPrimaryEntity(actionSuccess.getActionSpec().getRecommendation());
                if (pendingActionWorkloadTypes.contains(entity.getType())) {
                    final Long completionTime = actionSuccess.getActionSpec().getExecutionStep()
                                    .getCompletionTime();
                    final Long entityId = entity.getId();
                    Map<Long, Long> entityIdToActionIdMap = new HashMap<>();
                    entityIdToActionIdMap.put(entityId, actionId);
                    Map<Long, EntityPriceChange> entityPriceChangeMap =
                                                      getOnDemandRates(realTimeTopologyContextId,
                                                                       entityIdToActionIdMap);
                    final EntityPriceChange actionPriceChange = entityPriceChangeMap.get(actionId);
                    if (actionPriceChange != null) {
                        final SavingsEvent successEvent =
                                                    createActionEvent(entity.getId(),
                                                      completionTime,
                                                      ActionEventType.EXECUTION_SUCCESS,
                                                      actionId,
                                                      actionPriceChange);
                        entityEventsJournal.addEvent(successEvent);
                        entityActionStateMap.put(actionId, ActionState.SUCCEEDED);
                        logger.debug("Added action {} for entity {}, completion time {}, recommendation time {}, journal size {}",
                                    actionId, entityId, completionTime,
                                    actionSuccess.getActionSpec().getRecommendationTime(),
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
        Set<Long> newPendingActionUuids = new HashSet<>();
        Map<ActionSpec, Long> newPendingActionSpecsToEntityId = new HashMap<>();
        Map<Long, Long> newPendingActionsEntityIdToActionId = new HashMap<>();
        final List<ActionSpec> actionSpecs = new ArrayList<>();
        Iterator<FilteredActionResponse> responseIterator = getUpdatedActions(topologyContextId);
        while (responseIterator.hasNext()) {
            actionSpecs.addAll(responseIterator.next().getActionChunk().getActionsList().stream()
                            .map(ActionOrchestratorAction::getActionSpec)
                            .collect(Collectors.toList()));
            for (ActionSpec actionSpec : actionSpecs) {
                if (actionSpec == null || !actionSpec.hasRecommendation()) {
                    continue;
                }
                final Long actionId = actionSpec.getRecommendation().getId();
                ActionEntity entity;
                try {
                    entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
                } catch (UnsupportedActionException e) {
                    logger.warn("Cannot create action Savings event due to unsupported action"
                                    + " type for action {}",
                                actionId, e);
                    continue;
                }
                if (!pendingActionWorkloadTypes.contains(entity.getType())) {
                    continue;
                }
                final Long entityId = entity.getId();
                logger.debug("Processing savings for action {}, entity {}", actionId, entityId);
                newPendingActionUuids.add(actionId);
                // Since we're looping through actions here, it's more  efficient to maintain this local
                // mapping and reverse mapping for O(1) lookup, rather than searching through
                // one data structure during events creation or retrieval of costs.
                newPendingActionSpecsToEntityId.put(actionSpec, entityId);
                newPendingActionsEntityIdToActionId.put(entityId, actionId);
                logger.debug("Adding Pending Action {}, at {} for entity {}", actionId,
                             actionSpec.getRecommendationTime(),
                            entityId);
            }
            actionSpecs.clear();
        }

        // TODO: The way we're processing update and success, we may not need synchronization.
        // However this may need to be re-evaluated in the future, because of potential race conditions,
        // in the presence of multiple threads.

        // Add new pending action events.
        // Compare the old and new actions and create SavingsEvents for new ActionId's.
        // entriesOnlyOnLeft() returns newly added actions, and entriesOnlyOnRight()
        // returns actions no longer being generated by market.
        MapDifference<ActionSpec, Long> actionChanges = Maps.difference(newPendingActionSpecsToEntityId,
                                                            currentPendingActionSpecsToEntityId);
        Map<Long, EntityPriceChange> entityPriceChangeMap =
                                      getOnDemandRates(realTimeTopologyContextId,
                                                       newPendingActionsEntityIdToActionId);
        Set<SavingsEvent> newPendingActionEvents = new HashSet<>();
        actionChanges.entriesOnlyOnLeft().keySet().forEach(newActionSpec -> {
            final Long newActionId = newActionSpec.getRecommendationId();
            final ActionState actionState = newActionSpec.getActionState();
            final EntityPriceChange actionPriceChange = entityPriceChangeMap
                            .get(newPendingActionSpecsToEntityId.get(newActionSpec));
            if (actionPriceChange != null) {
                logger.debug("New action price change for {}, {}, {}, {}:",
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

        final EntityPriceChange emptyPriceChange = new EntityPriceChange.Builder()
                                                    .sourceOid(0L)
                                                    .sourceCost(0.0)
                                                    .destinationOid(0L)
                                                    .destinationCost(0.0)
                                                    .build();
        // Add events related to stale pending actions.
        if (!currentPendingActionSpecsToEntityId.isEmpty()) {
            Set<SavingsEvent> staleActionEvents = new HashSet<>();
            actionChanges.entriesOnlyOnRight().keySet().forEach(staleActionSpec -> {
                final Long staleActionId = staleActionSpec.getRecommendationId();
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
     * @return The FilteredActionRequest.
     */
    private FilteredActionRequest filteredActionRequest(final Long topologyContextId) {
        AtomicReference<String> cursor = new AtomicReference<>("0");
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
     * Get the set of market actions filtered by filters specified in FilteredActionRequest.
     *
     * @param topologyContextId The topology context id.
     * @return Iterable collection of FilteredActionResponse.
     */
    private Iterator<FilteredActionResponse> getUpdatedActions(@Nonnull final Long topologyContextId) {
        final FilteredActionRequest filteredActionRequest =
                                                          filteredActionRequest(topologyContextId);
        return actionsService.getAllActions(filteredActionRequest);
    }

    /**
     * Create a Savings Event on receiving an Action Event.
     *
     * @param entityId The target entity ID.
     * @param timestamp The time-stamp of the action event (execution completion time, reccommendation tie etc).
     * @param actionType The action type.
     * @param actionId the action ID.
     * @param priceChange the price change associated with the action.
     * @return The SavingsEvent.
     */
    private static SavingsEvent
            createActionEvent(Long entityId, Long timestamp, ActionEventType actionType,
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
     * Get on-demand template rates for a list of target entities.
     *
     * @param topologyContextId - topology context ID.
     * @param entityIdToActionIdMap - action id to entity id map of market actions.
     * @return Map of actionId to EntityPriceChange.
     */
    private Map<Long, EntityPriceChange> getOnDemandRates(@Nonnull Long topologyContextId,
                                          @Nonnull final Map<Long, Long> entityIdToActionIdMap) {
        Map<Long, EntityPriceChange> actionIdToEntityPriceChange = new HashMap<>();
        Set<Long> entityUuids = entityIdToActionIdMap.keySet();
        // Get the On Demand compute costs
        GetTierPriceForEntitiesRequest.Builder onDemandComputeCostsRequest =
                                                   GetTierPriceForEntitiesRequest
                                                       .newBuilder()
                                                       .addAllOids(entityUuids)
                                                       .setCostCategory(CostCategory.ON_DEMAND_COMPUTE);
        if (Objects.nonNull(topologyContextId)) {
            onDemandComputeCostsRequest.setTopologyContextId(topologyContextId);
        }
        GetTierPriceForEntitiesResponse onDemandComputeCostsResponse = costService
                        .getTierPriceForEntities(onDemandComputeCostsRequest.build());
        Map<Long, CurrencyAmount> beforeOnDemandComputeCostByEntityOidMap =
                                                  onDemandComputeCostsResponse
                                                     .getBeforeTierPriceByEntityOidMap();
        Map<Long, CurrencyAmount> afterComputeCostByEntityOidMap = onDemandComputeCostsResponse
                        .getAfterTierPriceByEntityOidMap();

        // Get the On Demand License costs
        // TODO:  Check if we need to include license costs for AWS/Azure and any additional costs for Azure.
        GetTierPriceForEntitiesRequest.Builder onDemandLicenseCostsRequest =
                                               GetTierPriceForEntitiesRequest
                                                   .newBuilder()
                                                   .addAllOids(entityUuids)
                                                   .setCostCategory(CostCategory.ON_DEMAND_LICENSE);
        if (Objects.nonNull(topologyContextId)) {
            onDemandLicenseCostsRequest.setTopologyContextId(topologyContextId);
        }
        GetTierPriceForEntitiesResponse onDemandLicenseCostsResponse = costService
                        .getTierPriceForEntities(onDemandLicenseCostsRequest.build());
        Map<Long, CurrencyAmount> beforeLicenseComputeCosts = onDemandLicenseCostsResponse
                        .getBeforeTierPriceByEntityOidMap();
        Map<Long, CurrencyAmount> afterLicenseComputeCosts = onDemandLicenseCostsResponse
                        .getAfterTierPriceByEntityOidMap();

        entityUuids.forEach(entityUuid -> {
            double totalCurrentOnDemandRate = 0;
            if (beforeOnDemandComputeCostByEntityOidMap != null
                && beforeOnDemandComputeCostByEntityOidMap.get(entityUuid) != null) {
                double amount = beforeOnDemandComputeCostByEntityOidMap.get(entityUuid).getAmount();
                totalCurrentOnDemandRate += amount;
            }
            if (beforeLicenseComputeCosts != null
                && beforeLicenseComputeCosts.get(entityUuid) != null) {
                double amount = beforeLicenseComputeCosts.get(entityUuid).getAmount();
                totalCurrentOnDemandRate += amount;
            }
            if (totalCurrentOnDemandRate == 0) {
                logger.error("Current On Demand rate for entity with oid {}, not found",
                             entityUuid);
                return;
            }

            double totalProjectedOnDemandRate = 0;
            if (afterComputeCostByEntityOidMap != null
                && afterComputeCostByEntityOidMap.get(entityUuid) != null) {
                double amount = afterComputeCostByEntityOidMap.get(entityUuid).getAmount();
                totalProjectedOnDemandRate += amount;
            }

            if (afterLicenseComputeCosts != null
                && afterLicenseComputeCosts.get(entityUuid) != null) {
                double amount = afterLicenseComputeCosts.get(entityUuid).getAmount();
                totalProjectedOnDemandRate += amount;
            }

            if (totalProjectedOnDemandRate == 0) {
                logger.error("Projected On Demand rate for entity with oid {}, not found",
                             entityUuid);
                return;
            }

            final EntityPriceChange actionPriceChange = new EntityPriceChange.Builder()
                            .sourceOid(0L)
                            .sourceCost(totalCurrentOnDemandRate)
                            .destinationOid(0L)
                            .destinationCost(totalProjectedOnDemandRate)
                            .build();
            actionIdToEntityPriceChange.put(entityIdToActionIdMap.get(entityUuid),
                                            actionPriceChange);
        });

        return actionIdToEntityPriceChange;
    }
}
