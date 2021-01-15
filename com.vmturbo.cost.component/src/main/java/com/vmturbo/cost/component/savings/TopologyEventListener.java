package com.vmturbo.cost.component.savings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Listens for topology event changes and inserts events into the internal savings event log.
 */
public class TopologyEventListener {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     * @param config entity savings configuration.
     */
    TopologyEventListener(EntitySavingsConfig config) {
        // BCTODO register for action and topology events.
        logger.debug("Topology event listener enabled.");
    }
}
