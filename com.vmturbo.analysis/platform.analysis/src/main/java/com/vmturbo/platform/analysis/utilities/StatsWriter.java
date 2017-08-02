package com.vmturbo.platform.analysis.utilities;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * A class to write data to file per market name.
 *
 * Writes queued writes to file.
 * Supports concurrent writes to the same file by multiple threads.
 * @author reshmakumar1
 *
 */

public class StatsWriter extends Thread {

    private BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1000);
    private static final Logger logger = LogManager.getLogger(StatsWriter.class);
    private int linesWritten = 0;

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                consume();
            }
            catch (InterruptedException e) {
                logger.warn("Stats consumer interrupted..");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * StatsWriter constructor
     *
     */
    @VisibleForTesting
    StatsWriter() {}

    /**
     * Add data to queue to be written to file.
     *
     * @param data data to write
     */
    public void add(StatsUtils data) {
        if (logger.isDebugEnabled()) {
            logger.debug("Stats Adding to queue ..");
        }

        try {
            queue.put(data);
        }
        catch (InterruptedException e) {
            logger.error("Stats Add interrupted .." + data);
        }
    }

    /**
     * Write queued writes to file.
     *
     * Writes one or more lines (rows).
     * @throws InterruptedException
     */
    private void consume() throws InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("Stats Processing ..");
        }

        try {
            // queue object is an instance of StatsUtils,
            // which contains info on which file to write to
            StatsUtils su = (StatsUtils)queue.take();
            su.flush(true);

            // used for unit test only
            synchronized (this) {
                linesWritten++;
            }
        }
        catch (InterruptedException e) {
            logger.info("Stats Consume interrupted ..");
        }
    }

    /**
     * Get number of lines written.
     *
     * Used by unit Test StatsTest.
     * @return linesWritten
     */
    public synchronized int getLinesWritten() {
        return linesWritten;
    }

    /**
     * Interrupt the writer thread.
     *
     */
    @Override
    public void interrupt() {
        Thread.currentThread().interrupt();
    }
}
