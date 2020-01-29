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
     */
    public void updateHealthStatus() {

        // we are going to monitor the total heap used %, instead of just the tenured generation as
        // we used to. This is a simpler and more universal algorithm than finding and tracking the
        // tenured generation usage %.
        Runtime rt = Runtime.getRuntime();
        long maxMem = rt.maxMemory();
        long freeMem = rt.freeMemory();
        long usedMem = maxMem - freeMem;
        double usageRatio = 1.0 * usedMem / maxMem;
        double usedPercentage = Math.round(10000.0 * usageRatio) / 100.0; // round to 2 decimals for display
        log.debug("MemoryMonitor: currently {}% used ({}/{}} {} free memory", usedPercentage,
                usedMem, maxMem, freeMem);

        // report an unhealthy status if we are over the allocation threshold
        if (usageRatio >= memUsedRatioAlertThreshold) {
            String message = String.format("Heap %.2f%% (%d/%d) used. %d free.",
                    usedPercentage, usedMem, maxMem, freeMem);
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

                    // if we are unhealthy, then any GC can clear the unhealthy state as long as our
                    // mem usage indicates we have room
                    if (! getHealthStatus().isHealthy()) {
                        updateHealthStatus();
                    } else {
                        // otherwise, if we are healthy, we are only looking for the "unhealthy" scenario
                        // where we don't have enough heap left after a full gc happens.
                        if (info.getGcAction().equals(MAJOR_GC_ACTION_LABEL)) {
                            lastFullGcCompletionTime.set(System.currentTimeMillis());
                            updateHealthStatus();
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
