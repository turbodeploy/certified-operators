package com.vmturbo.history.listeners;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.chunking.RemoteIterator;

/**
 * Base class for topology listeners.
 */
public class TopologyListenerBase {
    // logs for this are more useful when they show the subclass
    private Logger logger = LogManager.getLogger(getClass());

    /**
     * Skip processing of the rest of the topology in the iterator by consuming the chunks and
     * doing nothing with them.
     *
     * @param topologyLabel label for the topology, for logging
     * @param iterator      the topology to be skipped
     */
    protected void skipRestOfTopology(String topologyLabel, RemoteIterator<?> iterator) {
        if (iterator.hasNext()) {
            logger.warn("Skipping remaining entities in {}", topologyLabel);
        } else {
            return;
        }
        // drain the remote iterator
        Stopwatch watch = Stopwatch.createStarted();
        int n = 0;
        while (iterator.hasNext()) {
            try {
                n += iterator.nextChunk().size();
            } catch (Exception e) {
                logger.warn("Exception while skipping chunks in {}", topologyLabel, e);
                break;
            }
        }
        logger.info("Skipped {} entities of {} in {}", n, topologyLabel, watch);
    }
}
