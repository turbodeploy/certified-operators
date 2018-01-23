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
        DeadlockHealthMonitor deadlockDetector = new DeadlockHealthMonitor(POLLING_INTERVAL_SECS);

        // set up a deadlock
        DeadlockScenario deadlockScenario = new DeadlockScenario();

        // verify that the next check is unhealthy
        SimpleHealthStatus healthStatus = deadlockDetector.getStatusStream().blockFirst(Duration.ofSeconds(5));
        Assert.assertFalse(healthStatus.isHealthy());
        deadlockScenario.stop();
        healthStatus = deadlockDetector.getStatusStream().blockFirst(Duration.ofSeconds(5));
        Assert.assertTrue(healthStatus.isHealthy());
    }

    /**
     * We will use the classic scenario of two thieves double-crossing each other at the end of a
     * mission. The doublecross is only successful if one thief can possess both locks after the
     * mission ends.
     */
    static private class DeadlockScenario {
        Thread t1,t2;
        Thief thief1 = new Thief("thief1");
        Thief thief2 = new Thief("thief2");

        public DeadlockScenario() {
            CountDownLatch missionEnd = new CountDownLatch(2);
            t1 = new Thread(() -> {
                thief1.doublecross(missionEnd, thief2);
            });
            t2 = new Thread(() -> {
                thief2.doublecross(missionEnd, thief1);
            });
            t1.start();
            t2.start();
        }

        public void stop() {
            logger.info("Stopping deadlock scenario.");
            if ((t1 != null) && t1.isAlive()) {
                t1.interrupt();
            }
            if ((t2 != null) && t2.isAlive()) {
                t2.interrupt();
            }
            try {
                while (t1.isAlive() || t2.isAlive()) {
                    Thread.sleep(100);
                }

            } catch (InterruptedException ie) {

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
