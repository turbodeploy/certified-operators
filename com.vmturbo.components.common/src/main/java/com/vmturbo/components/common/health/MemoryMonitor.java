package com.vmturbo.components.common.health;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    // memory pool names differ slightly depending on which GC implementation is used. We default
    // to "G1 Old Gen" because we normally run with the G1 garbage collector in our production code.
    // But this may get reset to a different name in setupMonitor() if we detect the runtime pool
    // name is different.
    private static String oldGenPoolName = "G1 Old Gen";

    // string message returned by Java after major GC events.
    private static final String MAJOR_GC_ACTION_LABEL = "end of major GC";

    private Logger log = LogManager.getLogger();

    // this is the "old gen heap used ratio" threshold at which the memory monitor will start
    // reporting unhealthy statuses. It defaults to 95%.
    private final double memUsedRatioAlertThreshold;

    // The time at which the last full gc completed.
    private final AtomicLong lastFullGcCompletionTime = new AtomicLong();

    /**
     * Constructs a memory monitor with specified params. The monitor will report an unhealthy status
     * if the ratio of old gen used / old gen max exceeds the specified threshold.
     * @param memUsedRatioAlertThreshold The "used" ratio at which to trigger the unhealthy status.
     *                                   e.g. 1.0 = unhealthy when 100% of old gen is used. Default
     *                                   is 0.95.
     */
    public MemoryMonitor(double memUsedRatioAlertThreshold) {
        super("Memory");
        this.memUsedRatioAlertThreshold = memUsedRatioAlertThreshold;
        // memory monitor starts off healthy
        reportHealthy("Initializing.");
        setupMonitor();
    }

    /**
     * hook into the memory management jmx beans so we can track heap availability.
     */
    private void setupMonitor() {
        // listener that will process JMX notifications
        MemoryNotificationListener notificationListener = new MemoryNotificationListener();

        boolean oldGenPoolFound = false;
        // we are going to listen for both major and minor gc's.
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            log.info("Found GC Bean {} mem pools {}", gcBean::getName, gcBean::getMemoryPoolNames);
            ((NotificationEmitter) gcBean).addNotificationListener(notificationListener, null, null);
            // find the actual old gen mem pool name in this VM, in case it's not using the G1
            // collector.
            for (String memPoolName : gcBean.getMemoryPoolNames()) {
                if (memPoolName.endsWith("Old Gen") || memPoolName.endsWith("Tenured Gen")) {
                    log.info("Setting old gen mem pool name to {}", memPoolName);
                    oldGenPoolName = memPoolName;
                    oldGenPoolFound = true;
                    break;
                }
            }
        }

        if (oldGenPoolFound) {
            reportHealthy("Running");
        } else {
            reportHealthy("Old gen pool not found.");
        }
    }

    /**
     * This health check reports "unhealthy" when the ratio of old gen space available is less than
     * the threshold specified in the memory monitor configuration.
     */
    public void updateHealthStatus(MemoryUsage memoryUsage) {
        // report an unhealthy status if we are over the allocation threshold
        double usageRatio = 1.0 * memoryUsage.getUsed() / memoryUsage.getMax();
        double usedPercentage = 100.0 * usageRatio;
        log.debug("MemoryMonitor: currently {}% ({}/{}) used.", usedPercentage, memoryUsage.getUsed(), memoryUsage.getMax());
        if (usageRatio >= memUsedRatioAlertThreshold) {
            String message = String.format("Tenured heap %.2f%% (%d/%d) used.",
                    usedPercentage, memoryUsage.getUsed(), memoryUsage.getMax());
            reportUnhealthy(message);
        }
        else {
            reportHealthy(String.format("%.2f%% used.", usedPercentage));
        }
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

                    // get the old gen pool info
                    MemoryUsage oldGenUsage = info.getGcInfo().getMemoryUsageAfterGc().get(oldGenPoolName);
                    if (oldGenUsage == null) {
                        // don't process further if no old gen after-GC info available.
                        log.warn("Old Gen Memory Pool after GC information not available.");
                        return;
                    }

                    // if we are unhealthy, then any GC can clear the unhealthy state as long as our
                    // mem usage indicates we have room
                    if (! getHealthStatus().isHealthy()) {
                        updateHealthStatus(oldGenUsage);
                    } else {
                        // otherwise, if we are healthy, we are only looking for the "unhealthy" scenario
                        // where we don't have enough heap left after a full gc happens.
                        if (info.getGcAction().equals(MAJOR_GC_ACTION_LABEL)) {
                            lastFullGcCompletionTime.set(System.currentTimeMillis());
                            updateHealthStatus(oldGenUsage);
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
