package com.vmturbo.components.common.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.TimeUnit;

import com.vmturbo.components.common.metrics.ScheduledMetrics.ScheduledMetricsObserver;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * A metric observer for reporting JVM runtime metrics from the {@link RuntimeMXBean}. Currently
 * we are only reporting <i>"uptime"</i>. "Uptime" is useful because it can be compared with the
 * "application uptime" metric (see {@link ComponentLifespanMetrics}) reported from the same
 * component to determine if there might be a problem with application startup. e.g. a component
 * that doesn't start up correctly may report a JVM uptime value without an application uptime
 * value. (this component should also be reported by the health check as "Unhealthy" since the
 * execution status does not reach RUNNING)
 */
public class JVMRuntimeMetrics implements ScheduledMetricsObserver {
    private final RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

    private static final DataMetricGauge UPTIME_GAUGE = DataMetricGauge.builder()
            .withName("component_jvm_uptime_minutes")
            .withHelp("How many minutes the jvm for this component has been running.")
            .build()
            .register();

    public void observe() {
        long uptimeMs = runtimeMxBean.getUptime();
        // let's convert to a slightly more readable minutes unit
        double uptimeMinutes = TimeUnit.MILLISECONDS.toMinutes(uptimeMs);
        UPTIME_GAUGE.setData(uptimeMinutes);
    }
}
