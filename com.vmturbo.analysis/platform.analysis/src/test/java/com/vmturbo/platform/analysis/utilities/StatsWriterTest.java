package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


public class StatsWriterTest {

    StatsWriter writer = new StatsWriter();
    StatsUtils su = new StatsUtils("StatsUnitTest", true);
    static final Logger logger = LogManager.getLogger(StatsWriterTest.class);

    @Test
    public void testMultipleWrites() {


        final TimeUnit TIMEUNIT_INTERVAL = TimeUnit.MINUTES;
        final int numThreads = 10;

        class Processor implements Runnable {

            private int id;

            public Processor(int id) {
                this.id = id;
            }

            @Override
            public void run() {
                su.append("I am Thread: " + id);
                writer.add(su);
            }
        }

        if(!writer.isAlive()) {
            writer.start();
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 1; i <= numThreads; i++) {
            executor.submit(new Processor(i));
        }

        executor.shutdown();


        try {
            executor.awaitTermination(1, TIMEUNIT_INTERVAL);

        }
        catch (InterruptedException e) {
            logger.info("Stats Test interrupted..");
        }

        while (writer.getLinesWritten() < numThreads) {
            //wait
        }
        writer.interrupt();

        try {
            writer.join();
        }
        catch (InterruptedException e) {
            logger.info("Stats Test interrupted..");
        }

        assertEquals(writer.getLinesWritten(), numThreads);

    }

}