package com.vmturbo.cost.component.savings;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;

/**
 * Module to track entity realized/missed savings/investments stats.
 */
public class EntitySavingsTracker {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    private EntitySavingsConfig config;
    private ActionListener actionListener;
    private TopologyEventListener topologyEventListener;

    /**
     * Constructor.
     * @param config entity savings configuration.
     * @param aoClientConfig Action orchestrator client config, used for subscribing to AO events.
     * @param scheduledExecutorService Periodic task executor to run savings calculator.
     */
    EntitySavingsTracker(EntitySavingsConfig config,
            ActionOrchestratorClientConfig aoClientConfig,
            final ScheduledExecutorService scheduledExecutorService) {
        this.config = config;
        // If entity saving is disabled, exit.
        if (!config.isEnabled()) {
            logger.debug("Entity savings/investment tracking is disabled");
            return;
        }
        this.actionListener = new ActionListener(config);
        this.topologyEventListener = new TopologyEventListener(config);
        aoClientConfig.actionOrchestratorClient().addListener(this.actionListener);
        scheduledExecutorService.scheduleWithFixedDelay(this::processEvents, 0, 1, TimeUnit.HOURS);
        logger.debug("Entity savings/investment tracking enabled.");
    }

    /**
     * Process events posted to the internal state of each entity whose savings/investments are
     * being tracked.
     */
    private void processEvents() {
        logger.debug("Processing savings/investment.");
        logger.debug("Savings/investment processing complete.");
    }
}
