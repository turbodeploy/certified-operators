package com.vmturbo.cost.component.savings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Listens for topology event changes and inserts events into the internal savings event log.
 */
public class TopologyEventsPoller {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor.
     */
    TopologyEventsPoller() {
    }

    void poll() {
        logger.debug("Topology event poller getting TEP events.");
    }
}
