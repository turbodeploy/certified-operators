package com.vmturbo.components.common.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.junit.Test;

/**
 *
 */
public class ScheduledMetricsTest {

    @Test
    public void testUptimePerformance() {
        JVMRuntimeMetrics runtimeMetrics = new JVMRuntimeMetrics();
        // run in a loop and see how long it took
        long startTime = System.nanoTime();
        int iterations = 100000;
        for (int x = 0 ; x < iterations ; x++) {
            runtimeMetrics.observe();
        }
        long endTime = System.nanoTime();
        long timeTaken = endTime - startTime;
        double nanosPerIteration = 1.0 * timeTaken / iterations;
        String output = String.format("runtime metrics observations took %d total nanos, and %.2f nanos / invocation", timeTaken, nanosPerIteration );
        System.out.println(output);

        // how about direct invocation of uptime?
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        startTime = System.nanoTime();
        for (int x = 0 ; x < iterations ; x++) {
            runtimeMetrics.observe();
        }
        endTime = System.nanoTime();
        timeTaken = endTime - startTime;
        nanosPerIteration = 1.0 * timeTaken / iterations;
        output = String.format("direct invocation of RuntimeMXBean.uptime took %d total nanos, and %.2f nanos / invocation", timeTaken, nanosPerIteration );
        System.out.println(output);
    }
}
