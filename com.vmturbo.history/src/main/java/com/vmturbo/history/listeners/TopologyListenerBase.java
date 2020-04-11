package com.vmturbo.history.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for topology listeners.
 */
public class TopologyListenerBase {
    // logs for this are more useful when they show the subclass
    private Logger logger = LogManager.getLogger(getClass());
}
