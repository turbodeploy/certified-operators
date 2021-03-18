package com.vmturbo.cost.component.savings;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * TODO Remove this class or change the implementation to work with the new definition of EntityState and the new calculate API.
 */
class SavingsCalculator {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

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
     *
     * @param entityStates a map of entity states for entities whose states are being tracked
     * @param events list of events
     * @param periodStartTime start time of the period
     * @param periodEndTime end time of the period
     */
    public void calculate(@Nonnull Map<Long, EntityState> entityStates,
                   @Nonnull final List<SavingsEvent> events, long periodStartTime,
                    long periodEndTime) {
        logger.debug("Calculating savings/investment from {} to {}", periodStartTime, periodEndTime);
        // Initialize algorithm state from the entity state list
        Map<Long, Algorithm> states = entityStates.values().stream()
                .map(entityState -> {
                    Algorithm algorithmState = new Algorithm2(entityState.getEntityId(), periodStartTime);
                    algorithmState.initState(entityState);
                    return algorithmState;
                })
                .collect(Collectors.toMap(Algorithm::getEntityOid, Function.identity()));

        // Set of entity IDs of entities that have associated events
        Set<Long> entitiesWithEvents = new HashSet<>();

        // Process the events
        for (SavingsEvent event : events) {
            long timestamp = event.getTimestamp();
            long entityId = event.getEntityId();
            if (event.hasActionEvent()) {
                handleActionEvent(states, timestamp, entityId, event);
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
     * Handle action related events: RECOMMENDATION_ADDED, RECOMMENDATION_REMOVED, and
     * EXECUTION_SUCCESS.
     *
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent the savings event to process
     */
    private void handleActionEvent(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        ActionEvent event = savingsEvent.getActionEvent().get();
        ActionEventType eventType = event.getEventType();
        if (ActionEventType.EXECUTION_SUCCESS.equals(eventType)) {
            handleExecutionSuccess(states, timestamp, entityId, savingsEvent);
        } else if (ActionEventType.RECOMMENDATION_ADDED.equals(eventType)) {
            handleRecommendationAdded(states, timestamp, entityId, savingsEvent);
        } else if (ActionEventType.RECOMMENDATION_REMOVED.equals(eventType)) {
            handleRecommendationRemoved(states, timestamp, entityId, savingsEvent);
        } else {
            logger.warn("Dropping unhandled action event type {}", eventType);
        }
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
     */
    private void handleExecutionSuccess(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle ExecutionSuccess for {} at {}", entityId, timestamp);
        if (!savingsEvent.hasEntityPriceChange()) {
            logger.warn("Cannot track action execution for {} at {} - missing price data",
                    entityId, timestamp);
            return;
        }
         commonResizeHandler(states, timestamp, entityId,
                 savingsEvent.getEntityPriceChange().get(), false);
    }

    /**
     * Handle a resize recommendation that has not yet been executed.  This starts accumulating
     * missed savings or investments.
     * @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent savings event containing the recommendation
     */
    private void handleRecommendationAdded(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle RecommendationAdded for {} at {}", entityId, timestamp);
        // We have a recommendation for an entity, so we are going to start tracking savings/investments.  Get the
        // current savings for this entity, or create a new savings tracker for it.

        if (!savingsEvent.hasEntityPriceChange()) {
            logger.warn("Cannot track resize recommendation for {} at {} - missing price data",
                    entityId, timestamp);
            return;
        }
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
            algorithmState.setPowerFactor(0L);
            algorithmState.setDeletePending(true);
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
     */
    private void handleProviderChange(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, SavingsEvent event) {
        logger.debug("Handle ProviderChange for {} at {}", entityId, timestamp);
        if (event.getEntityPriceChange().isPresent()) {
            commonResizeHandler(states, timestamp, entityId, event.getEntityPriceChange().get(), true);
        } else {
            logger.warn("ProviderChange event for {} at {} is missing price data - skipping",
                    entityId, timestamp);
        }
    }

    /**
     * Shared logic to track a resize.  This can be called by the action execution success logic
     * (solicited resize) or when an entity has changed providers (unsolicited resize due to
     * discovery or action script action executed).
     *  @param states a map of algorithm states for entities whose states are being tracked
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param change price change information
     * @param activeRecommendationRequired true if an active recommendation must exist.  If true
     */
    private void commonResizeHandler(@Nonnull Map<Long, Algorithm> states,
            long timestamp, long entityId, EntityPriceChange change,
            boolean activeRecommendationRequired) {
        // If this is an action execution, it's okay that we haven't seen this entity yet.
        // Create state for it and continue.
        Algorithm algorithmState = getAlgorithmState(states, entityId, timestamp,
                !activeRecommendationRequired);
        if (algorithmState == null) {
            // An active recommendation is required, which means the entity state must already
            // exist.  If it does not, there is nothing to do.
            logger.debug("Not processing resize - state for entity {} is missing", entityId);
            return;
        }
        algorithmState.endSegment(timestamp);
        EntityPriceChange currentRecommendation = algorithmState.getCurrentRecommendation();
        if (currentRecommendation != null) {
            if (activeRecommendationRequired && !currentRecommendation.equals(change)) {
                // There's a current recommendation, but it doesn't match this resize, so ignore it
                return;
            }
            algorithmState.setCurrentRecommendation(null);
        } else if (activeRecommendationRequired) {
            // No active resize action, so ignore this.
            return;
        }
        algorithmState.addAction(change.getDelta());
    }
}
