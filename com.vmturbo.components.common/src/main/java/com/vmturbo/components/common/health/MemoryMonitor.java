package com.vmturbo.components.common.health;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.annotations.VisibleForTesting;
import com.sun.management.GarbageCollectionNotificationInfo;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.components.common.config.IConfigSource;

/**
 * MemoryMonitor will check memory availability after full GC's, and return an unhealthy status if
 * there is less than the specified ratio of old generation heap available <b><em>after a Full GC</em></b>.
 *
 * We ignore minor garbage collection results for the purposes of checking for unhealthy status,
 * since they can include a lot of collectible-but-not-collected objects. The heap utilization ratio
 * after a Full GC is a much stronger indicator of the amount of uncollectible heap. In addition,
 * the presence of a Full GC when using the G1 garbage collector (which we are using) is already a
 * clue about potential memory utilization issues, since the G1 garbage collector attempts to avoid
 * them altogether.
 *
 * Once the memory threshold has been exceeded and the monitor is reporting the unhealthy state, we
 * do start looking at minor GC results too. If free heap after either a minor or major GC shows that
 * heap usage is back under the threshold, the Memory Monitor will report the check as back to
 * healthy.
 *
 */
public class MemoryMonitor extends SimpleHealthStatusProvider {
    // string message returned by Java after major GC events.
    private static final String MAJOR_GC_ACTION_LABEL = "end of major GC";

    private Logger log = LogManager.getLogger();

    /**
     * When heap used % goes above this threshold, the memory monitor will report as unhealthy. This
     * is tracked as a ratio between 0.0 (0%) and 1.0 (100%). Default: 95%
     */
    public static String PROP_MAX_HEALTHY_HEAP_USED_RATIO = "maxHealthyHeapUsedRatio";
    private double maxHealthyHeapUsedRatio;

    /**
     * The MemoryMonitor will start to log used memory % (after major GC events) at INFO level after
     * the heap used % crosses this threshold, (as well as when it is above and goes back beneath
     * the threshold). Default: 80%
     */
    public static String PROP_START_LOGGING_AT_HEAP_USED_RATIO = "startLoggingAtHeapUsedRatio";
    private double startLoggingAtHeapUsedRatio;

    /**
     * Once memory logging kicks in (based on startLoggingAtUsedMemoryRatio) -- we will only log changes
     * based on a % change relative to the last logged amount. This is to help avoid overlogging when
     * usage is hovering within a band that is over the logging threshold. Default: 2%
     */
    public static String PROP_MIN_INTERESTING_HEAP_USED_RATIO_CHANGE = "minInterestingHeapUsedRatioChange";
    private double minInterestingHeapUsedRatioChange;

    // track the last amount of used heap we logged.
    private double lastLoggedHeapUsedRatio;

    // The time at which the last full gc completed.
    private final AtomicLong lastFullGcCompletionTime = new AtomicLong();

    /**
     * Constructs a memory monitor with specified params. The monitor will report an unhealthy status
     * if the ratio of old gen used / old gen max exceeds the specified threshold.
     * @param configSource a config property source.
     */
    public MemoryMonitor(IConfigSource configSource) {
        super("Memory");
        configure(configSource);
        // memory monitor starts off healthy
        reportHealthy("Initializing.");
        setupMonitor();
    }

    /**
     * Update the configuration properties used by this class.
     * @param configSource a {@link IConfigSource} to read config properties from.
     */
    public void configure(IConfigSource configSource) {
        this.maxHealthyHeapUsedRatio = configSource.getProperty(PROP_MAX_HEALTHY_HEAP_USED_RATIO, Double.class, 0.95);
        this.startLoggingAtHeapUsedRatio = configSource.getProperty(PROP_START_LOGGING_AT_HEAP_USED_RATIO, Double.class, 0.8);
        this.minInterestingHeapUsedRatioChange = configSource.getProperty(PROP_MIN_INTERESTING_HEAP_USED_RATIO_CHANGE, Double.class, 0.02);
        log.info("MemoryMonitor: configured with maxHealthyHeapUsedRatio:{} startLoggingAt:{} minInterestingChange:{}", maxHealthyHeapUsedRatio, startLoggingAtHeapUsedRatio, minInterestingHeapUsedRatioChange);
    }

    /**
     * Set up the MemoryMonitor by hooking into the memory management jmx beans so we can track heap
     * availability.
     *
     * @return true if the setup was successful, false otherwise.
     */
    private void setupMonitor() {
        // listener that will process JMX notifications
        MemoryNotificationListener notificationListener = new MemoryNotificationListener();

        List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        // first, try to find the tenured generation we want to monitor
        for (GarbageCollectorMXBean gcBean : gcMXBeans) {
            log.info("Found GC Bean {} mem pools {}", gcBean::getName, gcBean::getMemoryPoolNames);
            ((NotificationEmitter) gcBean).addNotificationListener(notificationListener, null, null);
        }

        reportHealthy("Running");
    }

    /**
     * This health check reports "unhealthy" when the ratio of heap space available is less than
     * the threshold specified in the memory monitor configuration.
     * @param gcInfo metadata about the gc action that triggered this update
     */
    public void updateHealthStatus(GarbageCollectionNotificationInfo gcInfo) {
        // we are going to monitor the total heap used %, instead of just the tenured generation as
        // we used to. This is a simpler and more universal algorithm than finding and tracking the
        // tenured generation usage %.
        Runtime rt = Runtime.getRuntime();
        long maxMem = rt.maxMemory();
        long totalMem = rt.totalMemory(); // amount of memory currently claimed by JVM
        long freeMem = rt.freeMemory(); // memory free within the claimed memory --NOT total free memory.
        long usedMem = totalMem - freeMem;
        long totalFreeMem = maxMem - usedMem; // total free memory available to JVM
        double usageRatio = 1.0 * usedMem / maxMem;
        double usedPercentage = Math.round(10000.0 * usageRatio) / 100.0; // round to 2 decimals for display
        String description = null; // we'll create this lazily and only want to do so once.

        // Now let's determine if this update represents an "interesting" change that we should log.
        // Reminder: HotSpot G1GC only does Full GC as a last resort, so a major GC
        // will typically not occur until heap is nearly full -- and we won't start logging
        // "interesting" changes until after the first major GC. However, with HotSpot we enable
        // verbose gc logging anyways so we don't need the extra info provided by our custom logging
        // here anyways.
        boolean isInterestingChange = isInterestingChange(usageRatio, lastLoggedHeapUsedRatio);
        Level logMessageLevel = isInterestingChange ? Level.INFO : Level.DEBUG;
        if (log.getLevel().isLessSpecificThan(logMessageLevel)) {
            // this message should appear in the log.
            description = createHeapUsageChangeDescription(gcInfo, usedPercentage, usedMem, maxMem, totalFreeMem);
            log.log(logMessageLevel, "MemoryMonitor: {}", description);
            lastLoggedHeapUsedRatio = usageRatio;
        }

        // report an unhealthy status if we are over the allocation threshold
        if (usageRatio >= maxHealthyHeapUsedRatio) {
            if (description == null) {
                description = createHeapUsageChangeDescription(gcInfo, usedPercentage, usedMem, maxMem, totalFreeMem);
            }
            reportUnhealthy(description);
        }
        else {
            reportHealthy(String.format("After %s, Heap %.2f%% used.", gcInfo.getGcAction(), usedPercentage));
        }
    }

    /**
     * Utility method to create a log message for a heap usage change.
     * @return the string description of the change.
     */
    private String createHeapUsageChangeDescription(GarbageCollectionNotificationInfo gcInfo,
                                double usedPercentage, long usedMem, long maxMem, long freeMem) {
        StringBuilder sb = new StringBuilder("After ").append(gcInfo.getGcAction())
                .append(" (").append(gcInfo.getGcInfo().getDuration()).append(" ms), ")
                .append("heap ").append(usedPercentage).append("% used ")
                .append("(").append(StringUtil.getHumanReadableSize(usedMem))
                .append("/").append(StringUtil.getHumanReadableSize(maxMem)).append(") ")
                .append(StringUtil.getHumanReadableSize(freeMem)).append(" free");
        return sb.toString();
    }

    /**
     * Utility method for determining if a change is considered "interesting" by this Memory Monitor
     * or not. A change is "interesting" if any of the following or true:
     * <ol>
     *     <li>The ratio has just transitioned above the logging threshold.</li>
     *     <li>The last reported ratio was above the logging threshold, and the new ratio represents
     *     a delta of at least the "minimum interesting change amount".</li>
     * </ol>
     * @param newUsageRatio a usage ratio to be checked for "interestingness".
     * @param lastReportedUsageRatio a "last reported" usage ratio to use as the basis
     *                               for comparison.
     * @return true, if we think the delta between the two values is "interesting". false, otherwise.
     */
     @VisibleForTesting
     boolean isInterestingChange(double newUsageRatio, double lastReportedUsageRatio) {
        if (newUsageRatio >= startLoggingAtHeapUsedRatio) {
            return (Math.abs(newUsageRatio - lastReportedUsageRatio) >= minInterestingHeapUsedRatioChange);
        } else {
            // when we're below the "start logging" ratio, we're only interested if the last reported
            // ratio was above the logging threshold and the delta is greater than the minimum
            // interesting delta.
            if (lastReportedUsageRatio >= startLoggingAtHeapUsedRatio) {
                // new is lower than the last reported so we'll just take the difference.
                return (lastReportedUsageRatio - newUsageRatio >= minInterestingHeapUsedRatioChange);
            }
        }
        return false;
    }

    @Override
    public SimpleHealthStatus reportHealthy(@Nonnull final String message) {
        return super.reportHealthy(String.format("%s [lastFullGC %d]",
            message, lastFullGcCompletionTime == null ? 0 : lastFullGcCompletionTime.get()));
    }

    @Override
    public SimpleHealthStatus reportUnhealthy(@Nonnull final String message) {
        return super.reportUnhealthy(String.format("%s [lastFullGC %d]",
            message, lastFullGcCompletionTime == null ? 0 : lastFullGcCompletionTime.get()));
    }

    /**
     * The MemoryNotificationListener processes JMX notifications about garbage collection events.
     *
     * It will extract the "after GC" memory usage data and pass it to the health evaluation function
     * depending on whether a major or minor GC has occurred, and if the monitor is currently healthy
     * or not.
     */
    class MemoryNotificationListener implements NotificationListener {
        @Override
        public void handleNotification(final Notification notification, final Object handback) {
            log.trace("JMX notification rcvd of type {} : {}", notification.getType(),
                    notification.getMessage());
            switch(notification.getType()) {
                case GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION:
                    GarbageCollectionNotificationInfo info
                        = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                    log.trace("Received GC {} notification name {} info {}.", info::getGcAction,
                            info::getGcName, info::getGcCause);

                    // if we are unhealthy, then any GC can clear the unhealthy state as long as our
                    // mem usage indicates we have room
                    if (! getHealthStatus().isHealthy()) {
                        updateHealthStatus(info);
                    } else {
                        // otherwise, if we are healthy, we are only looking for the "unhealthy" scenario
                        // where we don't have enough heap left after a full gc happens.
                        if (StringUtils.equals(info.getGcAction(), MAJOR_GC_ACTION_LABEL)) {
                            lastFullGcCompletionTime.set(System.currentTimeMillis());
                            updateHealthStatus(info);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
