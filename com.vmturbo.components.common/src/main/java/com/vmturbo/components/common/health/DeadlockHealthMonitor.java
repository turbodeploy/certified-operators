package com.vmturbo.components.common.health;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import org.apache.commons.lang.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Periodically (on 15 minute intervals by default) checks for deadlocked threads and will report
 * unhealthy if any are found.
 *
 * The check uses the java {@link ThreadMXBean} class to perform the actual deadlock check. The
 * check takes on the order of milliseconds to run, depending on how many threads are present.
 */
public class DeadlockHealthMonitor extends PollingHealthMonitor {
    private Logger log = LogManager.getLogger();

    // use JMX to check for deadlocks
    private final ThreadMXBean threadMxBean;

    public DeadlockHealthMonitor(double pollingIntervalSecs) {
        super("Deadlock Checker", pollingIntervalSecs);
        threadMxBean = ManagementFactory.getThreadMXBean();
    }

    @Override
    public void updateHealthStatus() {
        long[] deadlockedThreadIds = threadMxBean.findDeadlockedThreads();
        if (ArrayUtils.isEmpty(deadlockedThreadIds)) {
            reportHealthy();
            return;
        }
        // we have at least one deadlock. Report unhealthy status.
        StringBuilder messageBuilder = new StringBuilder("Deadlocked threads found (")
                .append(deadlockedThreadIds.length).append("): ");
        for (int x = 0 ; x < deadlockedThreadIds.length ; x++) {
            ThreadInfo threadInfo = threadMxBean.getThreadInfo(deadlockedThreadIds[x]);
            messageBuilder.append("[").append(threadInfo.getThreadName())
                    .append(" waiting for lock owned by ")
                    .append(threadInfo.getLockOwnerName())
                    .append("] ");
        }
        reportUnhealthy(messageBuilder.toString());
    }
}
