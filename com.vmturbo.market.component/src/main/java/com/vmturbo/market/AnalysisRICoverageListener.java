package com.vmturbo.market;

import static com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType.SOURCE_RI_COVERAGE_UPDATE;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.cost.api.CostNotificationListener;
import com.vmturbo.market.runner.Analysis;

/**
 * Listener for receiving notifications from the Cost component for source entity RI coverage
 * readiness. This notification indicates that the RI coverage information related to a specific
 * topology id and context is ready for querying and consumption.
 */
public class AnalysisRICoverageListener implements CostNotificationListener {

    private CostNotification costNotification;
    private final Lock notificationLock = new ReentrantLock();
    private final Condition received = notificationLock.newCondition();

    private final Logger logger = LogManager.getLogger();

    /**
     * Returns a Future with the cost notification corresponding to the topology of the Analysis.
     *
     * @param analysis that wants the cost notification.
     * @return Future with cost notification.
     */
    @Nonnull
    public Future<CostNotification> receiveCostNotification(@Nonnull final Analysis analysis) {
        return CompletableFuture.supplyAsync(() -> {
            boolean timedOut = false;
            WaitStatus waitStatus = null;
            notificationLock.lock();
            try {
                while (!timedOut) {
                    waitStatus = determineWaitStatus(costNotification, analysis);
                    if (!waitStatus.shouldWait) {
                        break;
                    }
                    // await() returns false if the wait time has elapsed, and returns true if
                    // signalAll() has been invoked on the received Condition
                    timedOut = !received.await(10, TimeUnit.MINUTES);
                }
                if (timedOut) {
                    return CostNotification.newBuilder().setStatusUpdate(StatusUpdate
                            .newBuilder().setStatus(Status.FAIL)
                            .setStatusDescription("Timed out waiting for Cost notification.")
                            .build()).build();
                } else {
                    if (waitStatus.costComponentAhead) {
                        return CostNotification.newBuilder().setStatusUpdate(StatusUpdate
                            .newBuilder().setStatus(Status.FAIL)
                            .setStatusDescription("Cost component sent notification of a topology "
                                + "with topology creation time greater than the topology creation time "
                                + "of the topology market is processing.")
                            .build()).build();
                    } else {
                        logger.debug("Analysis: {} received cost notification message: {}",
                            analysis.getTopologyInfo(), costNotification);
                        return costNotification;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Error while waiting for cost notification", e);
                return null;
            } finally {
                notificationLock.unlock();
            }
        });
    }

    /**
     * Determines if we need to wait for the cost notification or not.
     *
     * @param costNotification cost notification received
     * @param analysis         current analysis
     * @return an instance of {@link WaitStatus}.
     */
    private WaitStatus determineWaitStatus(final CostNotification costNotification,
            final Analysis analysis) {
        if (costNotification != null
            && costNotification.hasStatusUpdate()
            && costNotification.getStatusUpdate().getType() == SOURCE_RI_COVERAGE_UPDATE) {
            if (costNotification.getStatusUpdate().getTopologyId() == analysis.getTopologyId()) {
                return new WaitStatus(false, false);
            }
            if (costNotification.getStatusUpdate().hasTopologyCreationTime()
                && analysis.getTopologyInfo() != null
                && analysis.getTopologyInfo().hasCreationTime()
                && costNotification.getStatusUpdate().getTopologyCreationTime() > analysis.getTopologyInfo().getCreationTime()) {
                return new WaitStatus(false, true);
            }
        }
        return new WaitStatus(true, false);
    }

    /**
     * Listener callback function used by CostComponent to send the cost notification.
     *
     * @param costNotification The cost notification.
     */
    @Override
    public void onCostNotificationReceived(
            @Nonnull final CostNotification costNotification) {
        notificationLock.lock();
        try {
            if (costNotification != null && costNotification.hasStatusUpdate()
                    && costNotification.getStatusUpdate().getType() == SOURCE_RI_COVERAGE_UPDATE) {
                this.costNotification = costNotification;
            }
            received.signalAll();
        } finally {
            notificationLock.unlock();
            logger.debug("Listener received cost notification message: {}", costNotification);
        }
    }

    /**
     * Wait status is used to determine if we need to wait for cost notification or not.
     */
    private static class WaitStatus {
        private final boolean shouldWait;
        private final boolean costComponentAhead;

        private WaitStatus(boolean shouldWait, boolean costComponentAhead) {
            this.shouldWait = shouldWait;
            this.costComponentAhead = costComponentAhead;
        }
    }
}
