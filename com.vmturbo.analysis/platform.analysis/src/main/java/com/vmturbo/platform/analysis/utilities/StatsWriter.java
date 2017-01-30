package com.vmturbo.platform.analysis.utilities;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

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
    private static final Logger logger = Logger.getLogger(StatsWriter.class);
    private StatsUtils stats;
    private int linesWritten = 0;
    private String fileName;

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

    public StatsWriter(String statsFileName) {
        stats = new StatsUtils(statsFileName, true);
        fileName = statsFileName;
    }

    /**
     * Add data to queue to be written to file.
     *
     * @param data data to write
     */
    public void add(Object data) {
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
            stats.write(queue.take(), true);
            linesWritten++;
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
    public int getLinesWritten() {
        return linesWritten;
    }

    /**
     * Interrupt the writer thread.
     *
     */
    @Override
    public void interrupt() {
        StatsManager.getInstance().remove(fileName);
        Thread.currentThread().interrupt();
    }
}
