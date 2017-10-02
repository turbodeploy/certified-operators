package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;

/**
 * Listener to receive event-related notifications for action execution.
 */
public interface ActionExecutionListener {
    /**
     * A notification sent by the Topology Processor to report the progress
     * of an action being executed. This notification will be triggered when the
     * Topology Processor receives an update from a probe concerning the execution
     * progress of an action.
     *
     * @param actionProgress The progress notification for an action.
     */
    void onActionProgress(@Nonnull ActionProgress actionProgress);


    /**
     * A notification sent by the Topology Processor to report the successful
     * completion of an action being executed. This notification will be triggered
     * when the Topology Processor receives the corresponding success update from
     * a probe.
     *
     * @param actionSuccess The progress notification for an action.
     */
    void onActionSuccess(@Nonnull ActionSuccess actionSuccess);

    /**
     * A notification sent by the Topology Processor to report the unsuccessful
     * completion of an action being executed. This notification will be triggered
     * when the Topology Processor receives the corresponding failure update from
     * a probe.
     *
     * @param actionFailure The progress notification for an action.
     */
    void onActionFailure(@Nonnull ActionFailure actionFailure);
}
