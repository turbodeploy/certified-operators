package com.vmturbo.components.common.health;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DeadlockMonitorTest {
    private static Logger logger = LogManager.getLogger();

    private static final double POLLING_INTERVAL_SECS = 0.05;

    @Test
    public void testNoDeadlock() {
        DeadlockHealthMonitor deadlockDetector = new DeadlockHealthMonitor(POLLING_INTERVAL_SECS);
        // verify that the next check is healthy
        SimpleHealthStatus healthStatus = deadlockDetector.getStatusStream().blockFirst(Duration.ofSeconds(1));
        Assert.assertTrue(healthStatus.isHealthy());
    }

    @Test
    public void testDeadlock() {
        // set up the monitor
        final DeadlockHealthMonitor deadlockDetector = new DeadlockHealthMonitor(POLLING_INTERVAL_SECS);

        final DeadlockScenario deadlockScenario = new DeadlockScenario();
        try {
            // set up a deadlock
             deadlockScenario.start();

            // verify that the last check in the next 100 ms is unhealthy
            SimpleHealthStatus healthStatus = deadlockDetector.getStatusStream()
                    .take(Duration.ofMillis(100)).last(deadlockDetector.getHealthStatus()).block();
            Assert.assertFalse(healthStatus.isHealthy());

        } finally {
            deadlockScenario.stop();
        }
        // we should be back to clean health
        SimpleHealthStatus cleanHealthStatus = deadlockDetector.getStatusStream()
                .blockFirst(Duration.ofSeconds(5));
        Assert.assertTrue(cleanHealthStatus.isHealthy());
    }

    /**
     * We will use the classic scenario of two thieves double-crossing each other at the end of a
     * mission. The doublecross is only successful if one thief can possess both locks after the
     * mission ends.
     */
    static private class DeadlockScenario {
        final CountDownLatch missionEnd = new CountDownLatch(2);
        final Thief thief1 = new Thief("thief1");
        final Thief thief2 = new Thief("thief2");
        final Thread t1 = new Thread(() -> {
            thief1.doublecross(missionEnd, thief2);
        });
        final Thread t2 = new Thread(() -> {
            thief2.doublecross(missionEnd, thief1);
        });

        public void start() {
            t1.start();
            t2.start();
            try {
                // don't leave the method until the deadlock scenario starts
                missionEnd.await();
                // at this point we know both thieves are going for the double cross and will
                // deadlock. But there is a small possibility that the threads haven't officially
                // deadlocked by the time we return from this method and the next health check runs.

                // If we really want to be thorough, we can wait here until the
                // ThreadMxBean detects the deadlock, but at that point we aren't really
                // testing the test subject any more, but rather testing the test code, so let's
                // see if we can avoid it for now.
            } catch (InterruptedException ie) {

            }
        }

        public void stop() {
            logger.info("Stopping deadlock scenario.");
            if (t1.isAlive()) {
                t1.interrupt();
            }
            if (t2.isAlive()) {
                t2.interrupt();
            }
            try {
                while (t1.isAlive() || t2.isAlive()) {
                    Thread.sleep(100);
                }

            } catch (InterruptedException ie) {
                // no op -- if we are interrupted we are shutting down anyways
            }
            logger.info("Deadlock scenario stopped.");
        }

        static class Thief {
            String name;

            Lock lock = new ReentrantLock();
            public Thief(String name) { this.name = name; }

            public void doublecross(CountDownLatch missionEnd, Thief rival) {
                lock.lock();
                missionEnd.countDown();
                try {
                    missionEnd.await();
                    // I have one lock, now take the other one
                    logger.info(name +": I have one lock, now I want th'other.");
                    rival.pilfer();
                    // successful steal!
                } catch (InterruptedException ie) {
                    // no op
                    logger.info(name +" interrupt received.");
                }
            }

            public void pilfer() {
                try {
                    lock.lockInterruptibly();
                    logger.info(name + ": Arr, Ya got me lock!");
                } catch (InterruptedException ie) {
                    logger.info("Pilfer interrupted.");
                }
            }
        }

    }
}
