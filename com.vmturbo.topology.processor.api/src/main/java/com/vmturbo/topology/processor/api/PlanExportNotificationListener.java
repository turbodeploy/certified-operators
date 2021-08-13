package com.vmturbo.topology.processor.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;

/**
 * A listener for updates to the status of a plan export operation.
 */
public interface PlanExportNotificationListener {
    /**
     * Called to indicate that a particular {@link PlanDestination} has a new state.
     * This can be called multiple times for the same {@link PlanDestination}.
     *
     * @param planDestinationOid the OID of the plan destination to which the export is directed
     * @param status a status message with the updated state, percentage, and message
     */
    void onPlanExportStateChanged(long planDestinationOid, @Nonnull PlanExportStatus status);

    /**
     * Called to indicate that an export to a {@link PlanDestination} is making progress.
     *
     * @param planDestinationOid the OID of the plan destination to which the export is directed
     * @param progress the percentage of progress achieved
     * @param message a progress message
     */
    void onPlanExportProgress(long planDestinationOid, int progress, String message);
}
