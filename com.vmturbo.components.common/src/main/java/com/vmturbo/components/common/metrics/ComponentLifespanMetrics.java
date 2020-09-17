package com.vmturbo.components.common.metrics;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.metrics.ScheduledMetrics.ScheduledMetricsObserver;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * Reports the application startup time and application uptime (which is different from JVM uptime)
 * as scrapable metrics.
 *
 * Application startup time is primarily intended for diagnostics, performance analysis and
 * troubleshooting. E.g is the component taking an unusual amount of time to start up? Is it taking
 * longer than it used to?
 *
 * "Application uptime" is intended for diagnostics and can be used to detect components that have
 * crashed, by comparing uptimes across component instances. In a normal environment, all of the
 * application uptimes should show similar values since they are all initially started in the same
 * session. But if a component has crashed (and been auto-restarted as per our docker policy), it
 * will report a noticeably shorter uptime than the other components. So components with short
 * uptimes should be seen as likely to have either crashed or been manually reconfigured / restarted.
 */
public class ComponentLifespanMetrics implements ScheduledMetricsObserver,
        ApplicationListener<ContextRefreshedEvent> {
    private Logger logger = LogManager.getLogger();

    private static ComponentLifespanMetrics instance;

    private static final DataMetricGauge COMPONENT_STARTUPTIME_GAUGE = DataMetricGauge.builder()
            .withName("component_startuptime_secs")
            .withHelp("How many seconds it took for this component to finish starting up.")
            .build()
            .register();

    private static final DataMetricGauge COMPONENT_UPTIME_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "component_uptime_seconds")
            .withHelp("How many seconds the component instance has been up and running.")
            .build()
            .register();

    private long startedTime = 0;

    private ComponentLifespanMetrics() {}

    public static synchronized ComponentLifespanMetrics getInstance() {
        if (instance == null) {
            instance = new ComponentLifespanMetrics();
        }
        return instance;
    }

    @Override
    public void onApplicationEvent(final ContextRefreshedEvent applicationReadyEvent) {
        // determine how long it took to start the component. We can calculate this using the jvm
        // start time relative to now.
        long jvmStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        // this event corresponds to when we have started, so set the startedTime
        startedTime = System.currentTimeMillis();
        double startupTimeSecs = (startedTime - jvmStartTime) / 1000.0;
        COMPONENT_STARTUPTIME_GAUGE.setData(startupTimeSecs);
        logger.info("Application startup time calculated as {} secs", startupTimeSecs);
    }

    @Override
    public void observe() {
        // calculate application uptime based on startuptime.
        if (startedTime == 0) {
            // only measure component uptime if it's actually started
            COMPONENT_UPTIME_GAUGE.setData(0.);
        } else {
            double uptimeSecs = TimeUnit.MILLISECONDS.toSeconds (System.currentTimeMillis() - startedTime);
            COMPONENT_UPTIME_GAUGE.setData(uptimeSecs);
        }
    }

}
