package com.vmturbo.components.api.client;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.StackTrace;

/**
 * Utility to help drain {@link RemoteIterator}s across components.
 */
public class RemoteIteratorDrain {

    private static Logger logger = LogManager.getLogger();

    private RemoteIteratorDrain() { }

    /**
     * Drain the {@link RemoteIterator}. If the iterator throws any exception, swallow the
     * exception and return immediately. Intended to be used in message processing code when
     * we want to ignore the contents of the iterator.
     *
     * @param iterator The {@link RemoteIterator} to drain.
     * @param label The label to use for log messages.
     * @param expectEmpty If true, print a warning if the iterator is non-empty.
     */
    public static void drainIterator(@Nonnull final RemoteIterator<?> iterator,
                                     @Nonnull final String label,
                                     final boolean expectEmpty) {
        String caller = null;
        if (iterator.hasNext()) {
            if (expectEmpty) {
                caller = StackTrace.getCaller();
                logger.warn("Draining non-empty iterator for {} (caller: {})", label, caller);
            }
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
                if (caller == null) {
                    caller = StackTrace.getCaller();
                }
                logger.warn("Exception while skipping chunks for " + label + " at " + caller, e);
                break;
            }
        }
        logger.info("Skipped {} entities when draining iterator for {}. Took {}", n, label, watch);
    }
}
