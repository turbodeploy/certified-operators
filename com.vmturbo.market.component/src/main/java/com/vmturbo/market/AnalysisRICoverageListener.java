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
            CostNotification receivedNotification = null;
            boolean timedOut = false;
            notificationLock.lock();
            try {
                while (!timedOut && (costNotification == null
                        || costNotification.getStatusUpdate().getType() != SOURCE_RI_COVERAGE_UPDATE
                        || costNotification.getStatusUpdate().getTopologyId()
                        != analysis.getTopologyId())) {
                    // await() returns false if the wait time has elapsed, and returns true if
                    // signalAll() has been invoked on the received Condition
                    timedOut = !received.await(10, TimeUnit.MINUTES);
                }
                if (timedOut) {
                    return CostNotification.newBuilder().setStatusUpdate(StatusUpdate
                            .newBuilder().setStatus(Status.FAIL)
                            .setStatusDescription("Timed out waiting for Cost notification.")
                            .build()).build();
                }
                receivedNotification = costNotification;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Error while waiting for cost notification", e);
            } finally {
                notificationLock.unlock();
            }
            logger.debug("Analysis: {} received cost notification message: {}",
                    analysis.getTopologyInfo(), receivedNotification);
            return receivedNotification;
        });
    }

    /**
     * Listener callback function used by CostComponent to send the cost notification.
     *
     * @param costNotification The cost notification.
     */
    @Override
    public void onCostNotificationReceived(@Nonnull final CostNotification costNotification) {
        if (costNotification.getStatusUpdate().getType() == SOURCE_RI_COVERAGE_UPDATE) {
            notificationLock.lock();
            try {
                this.costNotification = costNotification;
                received.signalAll();
            } finally {
                notificationLock.unlock();
                logger.debug("Listener received cost notification message: {}", costNotification);
            }
        }
    }
}
