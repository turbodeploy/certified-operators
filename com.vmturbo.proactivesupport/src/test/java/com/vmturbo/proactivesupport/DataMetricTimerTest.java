package com.vmturbo.proactivesupport;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for DataMetricTimer.
 */
public class DataMetricTimerTest {

    private static final double NANOS_PER_SECOND = 1E9;

    private DataMetricTimer testTimer;

    /**
     * Utility function for calculating seconds elapsed from a nano start time.
     * @param startTimeNanos the start time, in nanos
     * @return the elapsed time -- in seconds
     */
    private double getElapsedSeconds(long startTimeNanos) {
        return (System.nanoTime() - startTimeNanos) / NANOS_PER_SECOND;
    }

    /**
     * Verify the timer duration by comparing the observation against a separately measured
     * duration.
     */
    @Test
    public void testSimplestTimer() {
        // super simple test
        testTimer = new DataMetricTimer();
        testTimer.start();
        try {
            Thread.sleep(20);
        } catch(InterruptedException ie) {
            // noop -- shouldn't affect the test results
        }
        double timerDuration = testTimer.observe();
        Assert.assertTrue(timerDuration > 0);
    }

    /**
     * Verify the timer behavior when autoclose is used.
     */
    @Test
    public void testAutoClose() {
        testTimer = new DataMetricTimer();
        try (DataMetricTimer timer = testTimer.start()) {
            Thread.sleep(20);
        } catch (InterruptedException ie) {
            // noop -- shouldn't affect test results
        }
        double duration = testTimer.getTimeElapsedSecs();
        Assert.assertTrue(duration > 0);
    }

    /**
     * Verify an unlabeled timer works properly.
     * @throws Exception
     */
    @Test
    public void testSummaryTimerNoLabels() {
        DataMetricSummary metric = DataMetricSummary.builder()
                .withName("testSummary")
                .withHelp("Help")
                .build();
        try(DataMetricTimer timer = metric.startTimer()) {
            Thread.sleep(20);
        } catch (InterruptedException ie) {
            // noop -- shouldn't affect test results
        }
        // we should see a single metric observation equalling the measured duration as a result.
        Assert.assertTrue(metric.getSum() > 0);
    }

    @Test
    public void testSummaryTimerWithLabels() {
        DataMetricSummary metric = DataMetricSummary.builder()
                .withName("testSummary")
                .withHelp("Help")
                .withLabelNames("label1","label2")
                .build();
        long startTime = System.nanoTime();
        try(DataMetricTimer timer = metric.labels("labelValue1","labelValue2").startTimer()) {
            Thread.sleep(20);
        } catch (InterruptedException ie) {
            // noop -- shouldn't affect test results
        }

        Assert.assertTrue(metric.labels("labelValue1", "labelValue2").getSum() > 0);
        Assert.assertEquals("A value under different labels was not observed",0.0,
                metric.labels("labelValue2","labelValue2").getSum(),0.00);
    }

    @Test
    public void testRunnable() {
        testTimer = new DataMetricTimer();

        testTimer.time( () -> {
            try {
                Thread.sleep(25);
            } catch( InterruptedException e ) {
                // noop
            }
        } );

        Assert.assertTrue(testTimer.getTimeElapsedSecs() > 0);
    }
}
