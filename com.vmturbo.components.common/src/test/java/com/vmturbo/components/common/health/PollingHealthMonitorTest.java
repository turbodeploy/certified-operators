package com.vmturbo.components.common.health;

import java.time.Duration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the PollingHealthMonitor class
 */
public class PollingHealthMonitorTest {

    // private class for testing the polling health monitor
    static private class TestPollingHealthMonitor extends PollingHealthMonitor {
        /**
         * Construct the instance with the selected polling interval
         *
         * @param intervalSecs the polling interval to use, in seconds
         */
        public TestPollingHealthMonitor(final double intervalSecs) {
            super(intervalSecs);
        }

        // the test health monitor will report a status based on the isHealthy boolean
        public boolean isHealthy = false;

        @Override
        public void updateHealthStatus() {
            if ( isHealthy ) {
                reportHealthy();
            } else {
                reportUnhealthy("Reasons.");
            }
        }
    };

    public TestPollingHealthMonitor healthMonitor;

    @Before
    public void setup() {
        healthMonitor = new TestPollingHealthMonitor(0.1);
    }

    /**
     * Test the scenario where the first/immediate health check fails
     */
    @Test
    public void testImmediateFailure() throws Exception {
        // verify that the next check is a failure
        SimpleHealthStatus nextStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(2));
        Assert.assertNotNull(nextStatus); // a null nextStatus here means the stream timed out
        Assert.assertFalse("The health status should be unhealthy.",nextStatus.isHealthy());
    }

    /**
     * Test a "healthy" result
     *
     * @throws Exception
     */
    @Test
    public void testHealthyCheck() throws Exception {
        // set the test monitor to report a healthy status next.
        healthMonitor.isHealthy = true;

        // verify that the next check is healthy
        SimpleHealthStatus nextStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(2));
        Assert.assertNotNull(nextStatus); // a null nextStatus here means the stream timed out
        Assert.assertTrue("The monitor should report healthy.",nextStatus.isHealthy());
    }

    /**
     * Ensure the polling happens on the expected interval
     *
     * @throws Exception
     */
    @Test
    public void testPolling() throws Exception {
        // set up a new monitor to test a 0.1 second interval
        healthMonitor = new TestPollingHealthMonitor(0.1);

        healthMonitor.isHealthy = true;

        // verify that the next check is healthy
        SimpleHealthStatus firstStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(2));
        Assert.assertNotNull(firstStatus); // a null nextStatus here could mean the stream timed out

        // now get another status and compare the time interval
        // wait for the next check to complete before beginning the test
        SimpleHealthStatus secondStatus = healthMonitor.getStatusStream().blockFirst(Duration.ofSeconds(2));
        Assert.assertNotNull(secondStatus); // a null nextStatus here could mean the stream timed out
        long durationMs = Duration.between(firstStatus.checkTime,secondStatus.checkTime).toMillis();
        System.out.println("poll took "+ durationMs +"ms");
        Assert.assertTrue("Next Health Check poll should happen within 500 ms", (durationMs <= 500));
    }

}
