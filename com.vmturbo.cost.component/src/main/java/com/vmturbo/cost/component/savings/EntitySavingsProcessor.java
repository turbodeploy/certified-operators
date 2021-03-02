package com.vmturbo.cost.component.savings;

import java.util.List;

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

    private RollupSavingsProcessor rollupProcessor;

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     *
     * @param entitySavingsTracker entitySavingsTracker
     * @param topologyEventsPoller topologyEventsPoller
     * @param rollupProcessor For rolling up savings records.
     */
    EntitySavingsProcessor(@Nonnull EntitySavingsTracker entitySavingsTracker,
                                  @Nonnull TopologyEventsPoller topologyEventsPoller,
            @Nonnull RollupSavingsProcessor rollupProcessor) {
        this.topologyEventsPoller = topologyEventsPoller;
        this.entitySavingsTracker = entitySavingsTracker;
        this.rollupProcessor = rollupProcessor;
    }

    /**
     * This method is invoked once an hour.
     */
    void execute() {
        logger.info("START: Processing savings/investment.");

        logger.debug("Calling TopologyEventPoller to get TEP events.");
        topologyEventsPoller.poll();

        logger.debug("Invoke EntitySavingsTracker to process events.");
        final List<Long> hourlyStatsTimes = entitySavingsTracker.processEvents();

        logger.debug("Invoking RollupSavingsProcessor to process rollups.");
        rollupProcessor.process(hourlyStatsTimes);

        logger.info("END: Savings/investment processing complete.");
    }
}
