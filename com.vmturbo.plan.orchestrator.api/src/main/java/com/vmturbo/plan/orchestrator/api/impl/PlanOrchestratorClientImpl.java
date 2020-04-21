package com.vmturbo.plan.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.plan.orchestrator.api.PlanListener;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;
import com.vmturbo.plan.orchestrator.api.ReservationListener;

/**
 * Implementation of plan orchestrator client.
 */
public class PlanOrchestratorClientImpl extends
        MulticastNotificationReceiver<PlanStatusNotification, PlanListener> implements PlanOrchestrator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The topic name for plan orchestrator statuses.
     */
    public static final String STATUS_CHANGED_TOPIC = "plan-orchestrator-status-changed";

    /**
     * The topic name for plan orchestrator to produce/consume reservation status changes.
     */
    public static final String RESERVATION_NOTIFICATION_TOPIC = "reservation-status-notifications";

    private final MulticastNotificationReceiver<ReservationChanges, ReservationListener> reservationNotificationReceiver;

    /**
     * Create an instance of {@link PlanOrchestratorClientImpl}.
     *
     * @param planMessageReceiver the message receiver for plan notifications.
     * @param reservationMessageReceiver the message receiver for reservation notifications.
     * @param executorService the executor service.
     * @param kafkaReceiverTimeoutSeconds the kafka message timeout.
     */
    public PlanOrchestratorClientImpl(@Nonnull final IMessageReceiver<PlanStatusNotification> planMessageReceiver,
                                      @Nullable final IMessageReceiver<ReservationChanges> reservationMessageReceiver,
                                      @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(planMessageReceiver, executorService, kafkaReceiverTimeoutSeconds, message -> {
            logger.debug("Received plan status {}", message);
            return listener -> {
                if (message.hasDelete()) {
                    listener.onPlanDeleted(message.getDelete());
                } else if (message.hasUpdate()) {
                    listener.onPlanStatusChanged(message.getUpdate());
                } else {
                    logger.warn("Unknown message type {}. Dropping message.", message.getTypeCase());
                }
            };
        });

        if (reservationMessageReceiver == null) {
            reservationNotificationReceiver = null;
        } else {
            reservationNotificationReceiver = new MulticastNotificationReceiver<>(reservationMessageReceiver, executorService,
                kafkaReceiverTimeoutSeconds, reservationStatusChanges -> l -> {
                logger.debug("Received reservation status changes: {}", reservationStatusChanges);
                l.onReservationChanged(reservationStatusChanges);
            });
        }
    }

    /**
     * Add a {@link PlanListener} for the PlanOrchestrator to listen to plan status changes.
     *
     * @param planListener the plan listener.
     */
    @Override
    public void addPlanListener(@Nonnull final PlanListener planListener) {
        addListener(planListener);
    }

    /**
     * Add a {@link ReservationListener} for PlanOrchestrator to listen to reservation status changes.
     *
     * @param reservationListener the reservation listener to add to the notification receiver.
     */
    @Override
    public void addReservationListener(@Nonnull final ReservationListener reservationListener) {
        reservationNotificationReceiver.addListener(reservationListener);
    }
}
