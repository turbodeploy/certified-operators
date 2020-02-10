package com.vmturbo.plan.orchestrator.reservation;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.ReservationDTO;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * ReservationNotificationSender captures reservation changes in PlanOrchestrator
 * and broadcasts these reservation changes to other XL components.
 */
public class ReservationNotificationSender extends ComponentNotificationSender<ReservationChanges> {

    private final IMessageSender<ReservationChanges> sender;

    /**
     * Create an instance of {@link ReservationNotificationSender}.
     *
     * @param sender the message sender that will be used to send messages.
     */
    public ReservationNotificationSender(@Nonnull IMessageSender<ReservationChanges> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * Called to indicate that a particular {@link ReservationDTO.ReservationChanges} has a new status.
     * This can be called multiple times for the same {@link ReservationDTO.ReservationChanges}, and there
     * is no guaranteed progression to the status changes (i.e. no formal state machine).
     *
     * <p>For example, {@link ReservationNotificationSender} sends an update to Kafka, which
     * could potentially take some time. However, implementations are encouraged to do heavy
     * lifting asynchronously to prevent stalling processing of reservation updates.</p>
     *
     * @param reservationChanges The modified {@link ReservationDTO.ReservationChanges}.
     * @throws CommunicationException If there is an error processing the reservation.
     */
    public void sendNotification(@Nonnull final ReservationChanges reservationChanges)
            throws CommunicationException {
        try {
            sendMessage(sender, reservationChanges);
        } catch (InterruptedException e) {
            // May not be necessary given that we have moved to Kafka implementation from
            // websocket implementation but leaving for now since it is baked into our current
            // paradigm of handling listener exceptions.
            // Reset the interrupted status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    protected String describeMessage(@Nonnull ReservationChanges reservationStatusNotification) {
        return ReservationChanges.class.getSimpleName() + "[" + reservationStatusNotification + "]";
    }
}
