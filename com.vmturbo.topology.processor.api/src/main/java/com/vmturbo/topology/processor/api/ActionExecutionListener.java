package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.ActionsLost;

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
    default void onActionProgress(@Nonnull ActionProgress actionProgress) {}


    /**
     * A notification sent by the Topology Processor to report the successful
     * completion of an action being executed. This notification will be triggered
     * when the Topology Processor receives the corresponding success update from
     * a probe.
     *
     * @param actionSuccess The progress notification for an action.
     */
    default void onActionSuccess(@Nonnull ActionSuccess actionSuccess) {}

    /**
     * A notification sent by the Topology Processor to report the unsuccessful
     * completion of an action being executed. This notification will be triggered
     * when the Topology Processor receives the corresponding failure update from
     * a probe.
     *
     * @param actionFailure The progress notification for an action.
     */
    default void onActionFailure(@Nonnull ActionFailure actionFailure) {}

    /**
     * A notification sent by the Topology Processor to report that it lost the state of some
     * or all of the in-progress actions. See {@link ActionsLost}. There will be no more updates
     * for those actions.
     *
     * @param notification The {@link ActionsLost} notification.
     */
    default void onActionsLost(@Nonnull ActionsLost notification) { }
}
