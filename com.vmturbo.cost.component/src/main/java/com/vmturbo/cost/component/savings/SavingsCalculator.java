package com.vmturbo.cost.component.savings;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ResourceCreationDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ResourceDeletionDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.EntityStateCache.EntityState;

/**
 * This class implements the algorithm for calculating entity savings and investments.
 */
class SavingsCalculator {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();
    private final EntityStateCache entityStateCache;

    /**
     * Constructor.
     *
     * @param entityStateCache entity state to apply events to.
     */
    SavingsCalculator(EntityStateCache entityStateCache) {
        this.entityStateCache = entityStateCache;
    }

    /**
     * Calculates savings and investments.
     *
     * @param events List of events
     * @param periodStartTime start time of the period
     * @param periodEndTime end time of the period
     */
    void calculate(@Nonnull final List<SavingsEvent> events,
            long periodStartTime, long periodEndTime) {
        logger.debug("Calculating savings/investment from {} to {}", periodStartTime, periodEndTime);
        for (SavingsEvent event : events) {
            long timestamp = event.getTimestamp();
            long entityId = event.getEntityId();
            if (event.hasActionEvent()) {
                handleActionEvent(timestamp, entityId, event);
            }
            if (event.hasTopologyEvent()) {
                handleTopologyEvent(timestamp, entityId, event);
            }
        }
        // Close out the period.
        entityStateCache.getAll()
                .forEach(entityState -> entityState.endPeriod(periodStartTime, periodEndTime));
    }

    /**
     * Handle action related events: RECOMMENDATION_ADDED, RECOMMENDATION_REMOVED, and
     * EXECUTION_SUCCESS.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent the savings event to process
     */
    private void handleActionEvent(long timestamp, long entityId, SavingsEvent savingsEvent) {
        ActionEvent event = savingsEvent.getActionEvent().get();
        ActionEventType eventType = event.getEventType();
        if (ActionEventType.EXECUTION_SUCCESS.equals(eventType)) {
            handleExecutionSuccess(timestamp, entityId, savingsEvent);
        } else if (ActionEventType.RECOMMENDATION_ADDED.equals(eventType)) {
            handleRecommendationAdded(timestamp, entityId, savingsEvent);
        } else if (ActionEventType.RECOMMENDATION_REMOVED.equals(eventType)) {
            handleRecommendationRemoved(timestamp, entityId, savingsEvent);
        } else {
            logger.warn("Dropping unhandled action event type {}", eventType);
        }
    }

    /**
     * Handle events from the topology event processor: RESOURCE_CREATION, RESOURCE_DELETION,
     * STATE_CHANGE, and PROVIDER_CHANGE.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param topologyEvent the topology event to process
     */
    private void handleTopologyEvent(long timestamp, long entityId, SavingsEvent topologyEvent) {
        TopologyEvent event = topologyEvent.getTopologyEvent().get();
        TopologyEventType eventType = event.getType();
        if (TopologyEventType.RESOURCE_CREATION.equals(eventType)) {
            handleResourceCreation(timestamp, entityId, event.getEventInfo().getResourceCreation());
        } else if (TopologyEventType.RESOURCE_DELETION.equals(eventType)) {
            handleResourceDeletion(timestamp, entityId, event.getEventInfo().getResourceDeletion());
        } else if (TopologyEventType.STATE_CHANGE.equals(eventType)) {
            handleStateChange(timestamp, entityId, event.getEventInfo().getStateChange());
        } else if (TopologyEventType.PROVIDER_CHANGE.equals(eventType)) {
            handleProviderChange(timestamp, entityId, topologyEvent);
        } else {
            logger.warn("Dropping unhandled topology event type {}", eventType);
        }
    }

    /**
     * Handle successful action execution.  This will stop accumulating any missed savings and
     * investments incurred by a previous resize recommendation and start accumulating realized
     * savings and investments.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent action execution details
     */
    private void handleExecutionSuccess(long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle ExecutionSuccess for {} at {}", entityId, timestamp);
        if (!savingsEvent.hasEntityPriceChange()) {
            logger.warn("Cannot track action execution for {} at {} - missing price data",
                    entityId, timestamp);
            return;
        }
         commonResizeHandler(timestamp, entityId, savingsEvent.getEntityPriceChange().get(), false);
    }

    /**
     * Handle a resize recommendation that has not yet been executed.  This starts accumulating
     * missed savings or investments.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param savingsEvent savings event containing the recommendation
     */
    private void handleRecommendationAdded(long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle RecommendationAdded for {} at {}", entityId, timestamp);
        // We have a recommendation for an entity, so we are going to start tracking savings/investments.  Get the
        // current savings for this entity, or create a new savings tracker for it.

        if (!savingsEvent.hasEntityPriceChange()) {
            logger.warn("Cannot track resize recommendation for {} at {} - missing price data",
                    entityId, timestamp);
            return;
        }
        EntityState entityState = entityStateCache.getEntityState(savingsEvent.getEntityId(), timestamp, true);

        // Close out the current segment and open a new with the new periodic missed savings/investment
        entityState.endSegment(timestamp);

        // If there is an existing recommendation, the new one overrides the old one.  Back it out.
        EntityPriceChange currentRecommendation = entityState.getCurrentRecommendation();
        if (currentRecommendation != null) {
            entityState.adjustCurrentMissed(-getSavings(currentRecommendation), -getInvestment(currentRecommendation));
        }

        EntityPriceChange newRecommendation = savingsEvent.getEntityPriceChange().get();
        entityState.adjustCurrentMissed(getSavings(newRecommendation), getInvestment(newRecommendation));

        // Save the current recommendation so that we can match it up with a future action execution.
        entityState.setCurrentRecommendation(newRecommendation);
    }

    private double getSavings(EntityPriceChange priceChange) {
        return -Math.min(0.0, priceChange.getDelta());
    }

    private double getInvestment(EntityPriceChange priceChange) {
        return Math.max(0.0, priceChange.getDelta());
    }

    private void handleRecommendationRemoved(long timestamp, long entityId, SavingsEvent savingsEvent) {
        logger.debug("Handle RecommendationRemoved for {} at {}", entityId, timestamp);
        EntityState entityState = entityStateCache.getEntityState(savingsEvent.getEntityId(), timestamp, true);

        // Close out the current segment and open a new with the new periodic missed savings/investment
        entityState.endSegment(timestamp);

        // If there is an existing recommendation, back it out.  In normal cases, this will reset
        // the current missed savings/investments to zero.
        // BCTODO What if the removed recommendation isn't the current one?
        EntityPriceChange currentRecommendation = entityState.getCurrentRecommendation();
        if (currentRecommendation != null) {
            entityState.adjustCurrentMissed(-getSavings(currentRecommendation), -getInvestment(currentRecommendation));
        }

        // Clear the current recommendation.
        entityState.setCurrentRecommendation(null);
    }

    /**
     * Handle entity created event.  We currently do nothing with these.  We rely on a resize
     * recommendation to start tracking an entity.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param topologyEvent topology event with details of the entity creation
     */
    private void handleResourceCreation(long timestamp, long entityId,
            ResourceCreationDetails topologyEvent) {
        logger.debug("Handle ResourceCreation for entity ID {} at {}", entityId, timestamp);
    }

    /**
     * Handle a resource deletion event.  We treat this as a poweroff event.  After the entity is
     * processed, its state will be removed.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param topologyEvent resource deletion details
     */
    private void handleResourceDeletion(long timestamp, long entityId,
            ResourceDeletionDetails topologyEvent) {
        logger.debug("Handle ResourceDeletion for {} at {}", entityId, timestamp);
        // Turn the power off to prevent further savings/investment accumulation and mark the entity
        // as inactive, which will cause its state to be removed after the next processing pass.
        EntityState entityState = entityStateCache.getEntityState(entityId, timestamp, false);
        if (entityState != null) {
            entityState.endSegment(timestamp);
            entityState.setPowerFactor(0L);
            entityState.setActive(false);
        }
    }

    /**
     * Handle power state change.  Powering off suspends accumulation of savings and investments.
     * Powering on resumes accumulation.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param event topology entity state change details
     */
    private void handleStateChange(long timestamp, long entityId, EntityStateChangeDetails event) {
        logger.debug("Handle StateChange for {} at {}", entityId, timestamp);
        // Locate existing state.  If we aren't tracking, then ignore the event
        EntityState entityState = entityStateCache.getEntityState(entityId);
        if (entityState == null) {
            return;
        }

        TopologyDTO.EntityState newState = event.getDestinationState();
        if (logger.isDebugEnabled()) {
            TopologyDTO.EntityState oldState = event.getSourceState();
            logger.debug("Handling power event for {}: {} -> {}", entityId, oldState, newState);
        }
        entityState.endSegment(timestamp);
        entityState.setPowerFactor(TopologyDTO.EntityState.POWERED_ON.equals(newState) ? 1L : 0L);
    }

    /**
     * Handle provider change.  This occurs when an entity has changed size due to a previous action
     * execution or a resize not due to an action execution.
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param event SavingsEvent containing details of the topology provider change as well as the
     *              price change information.
     */
    private void handleProviderChange(long timestamp, long entityId, SavingsEvent event) {
        logger.debug("Handle ProviderChange for {} at {}", entityId, timestamp);
        if (event.getEntityPriceChange().isPresent()) {
            commonResizeHandler(timestamp, entityId, event.getEntityPriceChange().get(), true);
        } else {
            logger.warn("ProviderChange event for {} at {} is missing price data - skipping",
                    entityId, timestamp);
        }
    }

    /**
     * Shared logic to track a resize.  This can be called by the action execution success logic
     * (solicited resize) or when an entity has changed providers (unsolicited resize due to
     * discovery or action script action executed).
     *
     * @param timestamp time of the event
     * @param entityId affected entity ID
     * @param change price change information
     * @param activeRecommendationRequired true if an active recommendation must exist.  If true
     *        and there is no outstanding resize action, this method does nothing.
     */
    private void commonResizeHandler(long timestamp, long entityId, EntityPriceChange change,
            boolean activeRecommendationRequired) {
        // If this is an action execution, it's okay that we haven't seen this entity yet.
        // Create state for it and continue.
        EntityState entityState = entityStateCache.getEntityState(entityId, timestamp, !activeRecommendationRequired);
        if (entityState == null) {
            // An active recommendation is required, which means the entity state must already
            // exist.  If it does not, there is nothing to do.
            logger.debug("Not processing resize - state for entity {} is missing", entityId);
            return;
        }
        entityState.endSegment(timestamp);
        EntityPriceChange currentRecommendation = entityState.getCurrentRecommendation();
        if (currentRecommendation != null) {
            // BCTODO in the code, this is dangerous if we are processing an action from before the
            //  feature was enabled.  Figure out what this means.
            if (activeRecommendationRequired && !currentRecommendation.equals(change)) {
                // There's a current recommendation, but it doesn't match this resize, so ignore it
                return;
            }
            // Back out missed savings/investment incurred by this recommendation.
            entityState.adjustCurrentMissed(-getSavings(currentRecommendation), -getInvestment(currentRecommendation));
            entityState.setCurrentRecommendation(null);
        } else if (activeRecommendationRequired) {
            // No active resize action, so ignore this.
            return;
        }
        entityState.adjustCurrent(change.getDelta());
    }
}
