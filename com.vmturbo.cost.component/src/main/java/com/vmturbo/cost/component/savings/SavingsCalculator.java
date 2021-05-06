package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.savings.Algorithm.SavingsInvestments;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;

/**
 * This class implements the algorithm for calculating entity savings and investments.
 */
class SavingsCalculator {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    // Current period end
    private long periodEndtime;

    /**
     * Constructor.
     */
    SavingsCalculator() {
    }

    /**
     * Gets state of entity given the oid.  Optionally creates a new entity state if the entity
     * doesn't currently exist.
     *
     * @param stateMap current map of entities keyed by entity ID.
     * @param entityOid Oid of entity to get state for.
     * @param segmentStart time that the segment was opened.
     * @param createIfNotFound if true, create the entity state if it doesn't already exist.
     * @return Instance of EntityState, can be null if not found.
     */
    @Nullable
    public Algorithm getAlgorithmState(@Nonnull Map<Long, Algorithm> stateMap, long entityOid,
            long segmentStart, boolean createIfNotFound) {
        Algorithm algorithmState = stateMap.get(entityOid);
        if (algorithmState == null && createIfNotFound) {
            algorithmState = new Algorithm2(entityOid, segmentStart);
            stateMap.put(entityOid, algorithmState);
        }
        return algorithmState;
    }

    /**
     * Write state modified by the algorithm back to the entity state.
     *
     * @param algorithmState algorithm state
     * @param entityStates map of entity states currently being tracked
     * @param entitiesWithEvents set of OIDs of entities that have an associated event in this period
     * @param periodStartTime timestamp of the start of the calculcation period
     * @param periodEndTime timestamp of the enf of the calculcation period
     */
    private void updateEntityState(Algorithm algorithmState,
            @Nonnull Map<Long, EntityState> entityStates, Set<Long> entitiesWithEvents,
            long periodStartTime, long periodEndTime) {
        // Check for an existing entity state. The state will not exist if this is newly-tracked.
        Long entityOid = algorithmState.getEntityOid();
        EntityState entityState = entityStates.get(entityOid);
        if (entityState == null) {
            entityState = new EntityState(entityOid);
            entityStates.put(entityOid, entityState);
        }
        entityState.setPowerFactor(algorithmState.getPowerFactor());
        entityState.setActionList(algorithmState.getActionList());
        entityState.setExpirationList(algorithmState.getExpirationList());
        entityState.setNextExpirationTime(algorithmState.getNextExpirationTime());
        entityState.setCurrentRecommendation(algorithmState.getCurrentRecommendation());
        entityState.setDeletePending(algorithmState.getDeletePending());
        entityState.setUpdated(entitiesWithEvents.contains(entityOid));
        long periodLength = periodEndTime - periodStartTime;
        if (periodLength <= 0) {
            logger.warn("Period start time {} is the same as or after the end time {}",
                    periodStartTime, periodEndTime);
            return;
        }
        SavingsInvestments realized = algorithmState.getRealized();
        SavingsInvestments missed = algorithmState.getMissed();
        if (entityState.getRealizedInvestments() != null || realized.getInvestments() != 0) {
            entityState.setRealizedInvestments(realized.getInvestments() / periodLength);
        }
        if (entityState.getRealizedSavings() != null || realized.getSavings() != 0) {
            entityState.setRealizedSavings(realized.getSavings() / periodLength);
        }
        if (entityState.getMissedInvestments() != null || missed.getInvestments() != 0) {
            entityState.setMissedInvestments(missed.getInvestments() / periodLength);
        }
        if (entityState.getMissedSavings() != null || missed.getSavings() != 0) {
            entityState.setMissedSavings(missed.getSavings() / periodLength);
        }
    }

    /**
     * Calculates savings and investments.
     * @param entityStates a map of entity states for entities whose states are being tracked
     * @param forcedEntityStates entity states that need to run even without a triggering event
     * @param events list of events
     * @param periodStartTime start time of the period
     * @param periodEndTime end time of the period
     */
    public void calculate(@Nonnull Map<Long, EntityState> entityStates,
                          @Nonnull Collection<EntityState> forcedEntityStates,
                          @Nonnull final List<SavingsEvent> events,
                          long periodStartTime, long periodEndTime) {
        logger.debug("Calculating savings/investment from {} to {}", periodStartTime, periodEndTime);
        this.periodEndtime = periodEndTime;
        // Convert the event list to a heap so that we can modify it while consuming it
        PriorityQueue<SavingsEvent> eventHeap =
                new PriorityQueue<>(SavingsEvent::compareConsideringTimestamp);
        eventHeap.addAll(events);
        // Generate action expired events
        createActionExpiredEvents(forcedEntityStates, eventHeap, periodEndTime);
        // Initialize algorithm state from the entity state list
        Map<Long, Algorithm> states = entityStates.values().stream()
                .map(entityState -> {
                    Algorithm algorithmState = new Algorithm2(entityState.getEntityId(), periodStartTime);
                    algorithmState.initState(periodEndTime, entityState);
                    return algorithmState;
                })
                .collect(Collectors.toMap(Algorithm::getEntityOid, Function.identity()));

        // Set of entity IDs of entities that have associated events
        Set<Long> entitiesWithEvents = new HashSet<>();

        // Process the events
        while (!eventHeap.isEmpty()) {
            SavingsEvent event = eventHeap.poll();
            long timestamp = event.getTimestamp();
            long entityId = event.getEntityId();
            if (event.hasActionEvent()) {
                SavingsEvent expirationEvent = handleActionEvent(states, timestamp, entityId, event);
                if (expirationEvent != null) {
                    eventHeap.offer(expirationEvent);
                }
            }
            if (event.hasTopologyEvent()) {
                handleTopologyEvent(states, timestamp, entityId, event);
            }
            entitiesWithEvents.add(entityId);
        }
        // Close out the period and update the entity state.  This also creates new entity state
        // for newly-tracked entities.
        states.values()
                .forEach(state -> {
                    state.endPeriod(periodStartTime, periodEndTime);
                    updateEntityState(state, entityStates, entitiesWithEvents, periodStartTime, periodEndTime);
                });
    }

    /**
     * Generate action expired actions for all entities that have expired actions.  Inserting these
     * actions into the event journal ensures that the expirations are processed in the proper
     * order relative to other events that were posted to the entity.
     *
     * @param forcedEntityStates list of entity states that are forced to run.  This list includes
     *                           entities that had an action in the previous period and entities
     *                           that have expired actions.
     * @param events current list of events.  Generated action expiration events will be added to
     *               this list.
     * @param periodEndTime end of the current period.  All actions with a timestamp before this
     *                      time will be expiring in this period and will generate an action
     *                      expired event.
     */
    private void createActionExpiredEvents(Collection<EntityState> forcedEntityStates,
            PriorityQueue<SavingsEvent> events, long periodEndTime) {
        for (EntityState entityState : forcedEntityStates) {
            long entityId = entityState.getEntityId();
            Iterator<Double> deltas = entityState.getActionList().iterator();
            for (Long expirationTime : entityState.getExpirationList()) {
                Double delta = deltas.next();
                if (expirationTime <= periodEndTime) {
                    events.offer(createActionExpiredEvent(entityId, expirationTime, delta));
                }
            }
        }
    }

    /**
     * Create an action expired event.
     *
     * <p>The EntityPriceChange isn't required by the algorithm, but it provides
     * useful information for the audit log to help correlate action creation
     * and expiration.  The source cost is forced to 0 and the destination cost
     * is set to the delta.  These are only set in order to force the calculated
     * delta in the EntityPriceChange to be correct.
     *
     * @param entityId entity Id to create the event for
     * @param expirationTime when the action expires
     * @param delta cost difference.
     * @return a SavingsEvent for the action expiration
     */
    private SavingsEvent createActionExpiredEvent(long entityId, Long expirationTime, Double delta) {
        EntityPriceChange entityPriceChange = new EntityPriceChange.Builder()
                .sourceCost(0)
                .destinationCost(delta)
                .build();
        SavingsEvent expiration = new SavingsEvent.Builder()
                .actionEvent(new ActionEvent.Builder()
                        .eventType(ActionEventType.ACTION_EXPIRED)
                        .actionId(entityId)
                        .expirationTime(expirationTime)
                        .build())
                .entityPriceChange(entityPriceChange)
                .entityId(entityId)
                .timestamp(expirationTime)
                .build();
        return expiration;
    }

    /**
     * Handle action related events: RECOMMENDATION_ADDED, RECOMMENDATION_REMOVED, and
     * EXECUTION_SUCCESS.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent the savings event to process
     * @return if non-null, the expiration event for this resize that must be processed this period.
     */
    @Nullable
    private SavingsEvent handleActionEvent(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        SavingsEvent expirationEvent = null;
        ActionEvent event = savingsEvent.getActionEvent().get();
        ActionEventType eventType = event.getEventType();
        if (ActionEventType.ACTION_EXPIRED.equals(eventType)) {
            handleExpiredAction(states, timestamp, entityId);
        } else if (ActionEventType.SCALE_EXECUTION_SUCCESS.equals(eventType)
                || ActionEventType.DELETE_EXECUTION_SUCCESS.equals(eventType)) {
            expirationEvent = handleExecutionSuccess(states, timestamp, entityId, savingsEvent);
        } else if (ActionEventType.RECOMMENDATION_ADDED.equals(eventType)) {
            handleRecommendationAdded(states, timestamp, entityId, savingsEvent);
        } else if (ActionEventType.RECOMMENDATION_REMOVED.equals(eventType)) {
            handleRecommendationRemoved(states, timestamp, entityId, savingsEvent);
        } else {
            logger.warn("Dropping unhandled action event type {}", eventType);
        }
        return expirationEvent;
    }

    private void handleExpiredAction(Map<Long, Algorithm> states, long timestamp, long entityId) {
        Algorithm algorithmState = getAlgorithmState(states, entityId, timestamp, false);
        if (algorithmState == null) {
            // An active recommendation is required, which means the entity state must already
            // exist.  If it does not, there is nothing to do.
            logger.debug("Not processing action expiration - state for entity {} is missing", entityId);
            return;
        }
        algorithmState.endSegment(timestamp);
        // This removes ALL expired actions on or before the timestamp, so it's possible that other
        // expired actions that are still in the event journal will also be removed.  This is okay.
        //  removeActionsOnOrBefore() for those events' timestamps will be a no-op.
        algorithmState.removeActionsOnOrBefore(timestamp);
    }

    /**
     * Handle events from the topology event processor: RESOURCE_CREATION, RESOURCE_DELETION,
     * STATE_CHANGE, and PROVIDER_CHANGE.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param topologyEvent the topology event to process
     */
    private void handleTopologyEvent(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent topologyEvent) {
        TopologyEvent event = topologyEvent.getTopologyEvent().get();
        TopologyEventType eventType = event.getType();
        if (TopologyEventType.RESOURCE_CREATION.equals(eventType)) {
            handleResourceCreation(timestamp, entityId);
        } else if (TopologyEventType.RESOURCE_DELETION.equals(eventType)) {
            handleResourceDeletion(states, timestamp, entityId);
        } else if (TopologyEventType.STATE_CHANGE.equals(eventType)) {
            handleStateChange(states, timestamp, entityId, event.getEventInfo().getStateChange());
        } else if (TopologyEventType.PROVIDER_CHANGE.equals(eventType)) {
            handleProviderChange(states, timestamp, entityId, topologyEvent);
        } else {
            logger.warn("Dropping unhandled topology event type {}", eventType);
        }
    }

    /**
     * Handle successful action execution.  This will stop accumulating any missed savings and
     * investments incurred by a previous resize recommendation and start accumulating realized
     * savings and investments.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent action execution details
     * @return if non-null, the expiration event for this resize that must be processed this period.
     */
    @Nullable
    private SavingsEvent handleExecutionSuccess(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle ExecutionSuccess for {} at {}", entityId, timestamp);
        if (!savingsEvent.hasEntityPriceChange()) {
            logger.warn("Cannot track action execution for {} at {} - missing price data",
                    entityId, timestamp);
            return null;
        }
        return commonResizeHandler(states, timestamp, entityId,
                savingsEvent.getEntityPriceChange().get(),
                savingsEvent.getActionEvent().get().getEventType(),
                savingsEvent.getActionEvent().get().getExpirationTime());
    }

    /**
     * Handle a resize or delete volume recommendation that has not yet been executed.  This starts
     * accumulating missed savings or investments.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent savings event containing the recommendation
     */
    private void handleRecommendationAdded(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle RecommendationAdded for {} at {}", entityId, timestamp);
        if (!savingsEvent.hasEntityPriceChange()) {
            logger.warn("Cannot track resize recommendation for {} at {} - missing price data",
                    entityId, timestamp);
            return;
        }
        // We have a recommendation for an entity, so we are going to start tracking missed
        // savings/investments.  Get the current savings for this entity, or create a new savings
        // tracker for it.
        Algorithm algorithmState = getAlgorithmState(states, savingsEvent.getEntityId(), timestamp, true);

        // Close out the current segment and open a new with the new periodic missed savings/investment
        algorithmState.endSegment(timestamp);

        // Save the current recommendation so that we can match it up with a future action execution.
        algorithmState.setCurrentRecommendation(savingsEvent.getEntityPriceChange().get());
    }

    private void handleRecommendationRemoved(@Nonnull Map<Long, Algorithm> entityStates,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle RecommendationRemoved for {} at {}", entityId, timestamp);
        Algorithm algorithmState = getAlgorithmState(entityStates, savingsEvent.getEntityId(), timestamp, true);

        // Close out the current segment and open a new with the new periodic missed savings/investment
        algorithmState.endSegment(timestamp);

        // Clear the current recommendation.
        algorithmState.setCurrentRecommendation(null);
    }

    /**
     * Handle entity created event.  We currently do nothing with these.  We rely on a resize
     * recommendation to start tracking an entity.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     */
    private void handleResourceCreation(long timestamp, long entityId) {
        logger.debug("Handle ResourceCreation for entity ID {} at {}", entityId, timestamp);
    }

    /**
     * Handle a resource deletion event.  We treat this as a poweroff event.  After the entity is
     * processed, its state will be removed.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     */
    private void handleResourceDeletion(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId) {
        logger.debug("Handle ResourceDeletion for {} at {}", entityId, timestamp);
        // Turn the power off to prevent further savings/investment accumulation and mark the entity
        // as inactive, which will cause its state to be removed after the next processing pass.
        Algorithm algorithmState = getAlgorithmState(states, entityId, timestamp, false);
        if (algorithmState != null) {
            algorithmState.endSegment(timestamp);
            // In order to track savings for entities that have been removed, we must retain their
            // state.  To prevent further accrual of stats, clear the action list and set the
            // power state to off.
            algorithmState.setPowerFactor(0L);
            algorithmState.clearActionList();
        }
    }

    /**
     * Handle power state change.  Powering off suspends accumulation of savings and investments.
     * Powering on resumes accumulation.
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param event topology entity state change details
     */
    private void handleStateChange(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, EntityStateChangeDetails event) {
        logger.debug("Handle StateChange for {} at {}", entityId, timestamp);
        // Locate existing state.  If we aren't tracking, then ignore the event
        Algorithm algorithmState = getAlgorithmState(states, entityId, 0L, false);
        if (algorithmState == null) {
            return;
        }
        TopologyDTO.EntityState newState = event.getDestinationState();
        if (logger.isDebugEnabled()) {
            TopologyDTO.EntityState oldState = event.getSourceState();
            logger.debug("Handling power event for {}: {} -> {}", entityId, oldState, newState);
        }
        algorithmState.endSegment(timestamp);
        algorithmState
                .setPowerFactor(TopologyDTO.EntityState.POWERED_ON.equals(newState) ? 1L : 0L);
    }

    /**
     * Handle provider change.  This occurs when an entity has changed size due to a previous action
     * execution or a resize not due to an action execution.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param event SavingsEvent containing details of the topology provider change as well as the
     *              price change information.
     * @return if non-null, the expiration event for this resize that must be processed this period.
     */
    @Nullable
    private SavingsEvent handleProviderChange(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent event) {
        logger.debug("Handle ProviderChange for {} at {}", entityId, timestamp);
        // TODO: Make expiration be an optional field in SavingsEvent rather than just ActionEvent,
        // and make TEP pass the expiration in based on action/ entity type.  For now using this
        // temporary large value for expiration.
        final long expirationTime = LocalDateTime.now().plusYears(1000L).toInstant(ZoneOffset.UTC)
                        .toEpochMilli();
        if (event.getEntityPriceChange().isPresent()) {
            return commonResizeHandler(states, timestamp, entityId,
                                       event.getEntityPriceChange().get(),
                                       ActionEventType.SCALE_EXECUTION_SUCCESS,
                                       Optional.of(expirationTime));
        } else {
            logger.warn("ProviderChange event for {} at {} is missing price data - skipping",
                    entityId, timestamp);
        }
        return null;
    }

    /**
     * Shared logic to track a resize.  This can be called by the action execution success logic
     * (solicited resize) or when an entity has changed providers (unsolicited resize due to
     * discovery or action script action executed).
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param change price change information
     * @param actionEventType Triggering action type (resize success or volume delete success)
     * @param expirationTimestamp Time in milliseconds after execution when the action will expire.
     * @return if non-null, the expiration event for this resize that must be processed this period.
     */
    @Nullable
    private SavingsEvent commonResizeHandler(@Nonnull Map<Long, Algorithm> states, long timestamp,
                                             long entityId, EntityPriceChange change,
                                             ActionEventType actionEventType,
                                             final Optional<Long> expirationTimestamp) {
        // If this is an action execution, it's okay that we haven't seen this entity yet.
        // Create state for it and continue.
        Algorithm algorithmState = getAlgorithmState(states, entityId, timestamp, false);
        if (algorithmState == null || !expirationTimestamp.isPresent()) {
            // An active recommendation is required, which means the entity state must already
            // exist.  If it does not, there is nothing to do.
            logger.debug("Not processing resize - state for entity {} is missing", entityId);
            return null;
        }
        EntityPriceChange currentRecommendation = algorithmState.getCurrentRecommendation();
        if (currentRecommendation == null) {
            // No active resize action, so ignore this.
            logger.warn("Not processing resize of {} tp {} for {} - no matching recommendation",
                    change.getSourceOid(), change.getDestinationOid(), entityId);
            return null;
        }
        algorithmState.endSegment(timestamp);
        algorithmState.setCurrentRecommendation(null);
        // If this is a volume delete, remove all existing active actions.
        if (ActionEventType.DELETE_EXECUTION_SUCCESS.equals(actionEventType)) {
            algorithmState.clearActionList();
        }

        algorithmState.addAction(currentRecommendation.getDelta(), expirationTimestamp.get());
        // If this action will expire in this same period, insert its expiration event now.
        return expirationTimestamp.get() < periodEndtime
                        ? createActionExpiredEvent(entityId, expirationTimestamp.get(),
                                                   change.getDelta())
                        : null;
    }
}
