package com.vmturbo.plan.orchestrator.plan;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO;

/**
 * A listener for updates to the status of a {@link PlanDTO.PlanInstance}.
 */
public interface PlanStatusListener {

    /**
     * Called to indicate that a particular {@link PlanDTO.PlanInstance} has a new status.
     * This can be called multiple times for the same {@link PlanDTO.PlanInstance}, and there
     * is no guaranteed progression to the status changes (i.e. no formal state machine).
     *
     * Right now there are no requirements regarding the complexity of implementations of this
     * method. For example, {@link PlanNotificationSender} sends an update to Kafka, which
     * could potentially take some time. However, implementations are encouraged to do heavy
     * lifting asynchronously to prevent stalling processing of plan updates.
     *
     * @param plan The modified {@link PlanDTO.PlanInstance}.
     * @throws PlanStatusListenerException If there is an error processing the plan.
     */
    void onPlanStatusChanged(@Nonnull PlanDTO.PlanInstance plan) throws PlanStatusListenerException;

    /**
     * Called to indicate that a particular plan got deleted.
     *
     * @param plan The {@link PlanDTO.PlanInstance} that got deleted.
     * @throws PlanStatusListenerException If there is an error processing the notification.
     */
    void onPlanDeleted(@Nonnull PlanDTO.PlanInstance plan) throws PlanStatusListenerException;

    /**
     * An exception that indicates an error processing a status update to a
     * {@link PlanDTO.PlanInstance}.
     */
    class PlanStatusListenerException extends Exception {
        public PlanStatusListenerException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }
}
