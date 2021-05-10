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
            notificationLock.lock();
            try {
                while (!timedOut && !isOfInterest(costNotification, analysis)) {
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
                    logger.debug("Analysis: {} received cost notification message: {}",
                            analysis.getTopologyInfo(), costNotification);
                    return costNotification;
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
     * Determines whether this notification is one that we care about for the given analysis.
     *
     * @param costNotification cost notification received
     * @param analysis         current analysis
     * @return true if this notification applies
     */
    private boolean isOfInterest(final CostNotification costNotification,
            final Analysis analysis) {
        return costNotification != null
                && costNotification.hasStatusUpdate()
                && costNotification.getStatusUpdate().getType() == SOURCE_RI_COVERAGE_UPDATE
                && costNotification.getStatusUpdate().getTopologyId() == analysis.getTopologyId();
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
}
