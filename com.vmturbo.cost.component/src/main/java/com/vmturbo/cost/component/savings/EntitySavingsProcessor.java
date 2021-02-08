package com.vmturbo.cost.component.savings;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class implements the task that is executed periodically (once an hour) to process entity
 * events and create savings and investment statistics.
 */
class EntitySavingsProcessor {

    private TopologyEventsPoller topologyEventsPoller;

    private EntitySavingsTracker entitySavingsTracker;

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param entitySavingsTracker entitySavingsTracker
     * @param topologyEventsPoller topologyEventsPoller
     */
    EntitySavingsProcessor(@Nonnull EntitySavingsTracker entitySavingsTracker,
                                  @Nonnull TopologyEventsPoller topologyEventsPoller) {
        this.topologyEventsPoller = topologyEventsPoller;
        this.entitySavingsTracker = entitySavingsTracker;
    }

    /**
     * This method is invoked once an hour.
     */
    void execute() {
        logger.debug("Processing savings/investment.");

        logger.debug("Calling TopologyEventPoller to get TEP events.");
        topologyEventsPoller.poll();

        logger.debug("Invoke EntitySavingsTracker to process events.");
        entitySavingsTracker.processEvents();

        logger.debug("Savings/investment processing complete.");
    }
}
