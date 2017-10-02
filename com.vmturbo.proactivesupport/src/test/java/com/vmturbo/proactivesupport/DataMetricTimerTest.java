package com.vmturbo.proactivesupport;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for DataMetricTimer.
 */
public class DataMetricTimerTest {

    private static final double NANOS_PER_SECOND = 1E9;
    private static final double TIMER_ACCURACY = 0.008;

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
        long startTime = System.nanoTime();
        testTimer.start();
        try {
            Thread.sleep(20);
        } catch(InterruptedException ie) {
            // noop -- shouldn't affect the test results
        }
        double independentlyMeasuredDurationSecs = getElapsedSeconds(startTime);
        double timerDuration = testTimer.observe();
        Assert.assertEquals(independentlyMeasuredDurationSecs, timerDuration, TIMER_ACCURACY);
    }

    /**
     * Verify the timer behavior when autoclose is used.
     */
    @Test
    public void testAutoClose() {
        testTimer = new DataMetricTimer();
        long startTime = System.nanoTime();
        try (DataMetricTimer timer = testTimer.start()) {
            Thread.sleep(20);
        } catch (InterruptedException ie) {
            // noop -- shouldn't affect test results
        }
        double independentlyMeasuredDurationSecs = getElapsedSeconds(startTime);
        double duration = testTimer.getTimeElapsedSecs();
        Assert.assertEquals(independentlyMeasuredDurationSecs, duration, TIMER_ACCURACY);
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
        long startTime = System.nanoTime();
        try(DataMetricTimer timer = metric.startTimer()) {
            Thread.sleep(20);
        } catch (InterruptedException ie) {
            // noop -- shouldn't affect test results
        }

        double independentlyMeasuredDurationSecs = getElapsedSeconds(startTime);
        // we should see a single metric observation equalling the measured duration as a result.
        Assert.assertEquals(independentlyMeasuredDurationSecs, metric.getSum(), TIMER_ACCURACY);
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

        // we should see a single observation matching the independently measure duration as a result.
        double independentlyMeasuredDurationSecs = getElapsedSeconds(startTime);
        Assert.assertEquals(independentlyMeasuredDurationSecs,
                metric.labels("labelValue1", "labelValue2").getSum(), TIMER_ACCURACY);
        Assert.assertEquals("A value under different labels was not observed",0.0,
                metric.labels("labelValue2","labelValue2").getSum(),0.00);
    }

    @Test
    public void testRunnable() {
        testTimer = new DataMetricTimer();

        long startTime = System.nanoTime();
        testTimer.time( () -> {
            try {
                Thread.sleep(25);
            } catch( InterruptedException e ) {
                // noop
            }
        } );

        // we should see a single observation matching the independently measure duration as a result.
        double independentlyMeasuredDurationSecs = getElapsedSeconds(startTime);
        Assert.assertEquals("Timed runnable should match thread sleep time.",
                independentlyMeasuredDurationSecs, testTimer.getTimeElapsedSecs(), TIMER_ACCURACY);
    }
}
