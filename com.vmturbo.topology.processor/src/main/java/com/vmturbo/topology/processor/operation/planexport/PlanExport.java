package com.vmturbo.topology.processor.operation.planexport;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.PlanExport.PlanExportResponse;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportResponse.PlanExportResponseState;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.Operation;

/**
 * An action operation on a target.
 */
public class PlanExport extends Operation {
    private final long destinationId;

    private int progress = 0;
    private String description = "Sending data to target";
    private PlanExportResponseState state = PlanExportResponseState.IN_PROGRESS;

    private static final DataMetricSummary PLAN_EXPORT_DURATION_SECONDS = DataMetricSummary.builder()
        .withName("tp_plan_export_duration_seconds")
        .withHelp("Duration of a plan export in the Topology Processor.")
        .build()
        .register();

    private static final DataMetricCounter PLAN_EXPORT_STATUS_COUNTER = DataMetricCounter.builder()
        .withName("tp_plan_export_status_total")
        .withHelp("Status of all completed plan exports.")
        .withLabelNames("status")
        .build()
        .register();

    /**
     * Constructs action operation.
     *
     * @param destinationId the plan destinatiom OID
     * @param probeId probe OID
     * @param targetId target OID
     * @param identityProvider identity provider to create a unique ID
     */
    public PlanExport(final long destinationId,
                      final long probeId,
                      final long targetId,
                      @Nonnull final IdentityProvider identityProvider) {
        super(probeId, targetId, identityProvider, PLAN_EXPORT_DURATION_SECONDS);
        this.destinationId = destinationId;
    }

    @Override
    public String toString() {
        return new StringBuilder()
            .append("Plan Export to destination ")
            .append(destinationId)
            .append(": ")
            .append(super.toString()).toString();
    }

    /**
     * Updates action progress from the specified action response object.
     *
     * @param progress action response to extract progress information from
     */
    public void updateProgress(final PlanExportResponse progress) {
        this.progress = progress.getProgress();
        this.description = progress.getDescription();
        this.state = progress.getState();
    }

    /**
     * Get the destination to which this plan export is to be directed.
     *
     * @return the oid of the plan destination.
     */
    public long getDestinationId() {
        return destinationId;
    }

    /**
     * Get the progress of the plan export.
     *
     * @return the progress as a percentage
     */
    public int getProgress() {
        return progress;
    }

    /**
     * Get a string describing giving more details about the progress and state of the export.
     *
     * @return a description string
     */
    @Nonnull
    public String getDescription() {
        return description;
    }

    /**
     * Get the state of the export operation.
     *
     * @return the state of the export operation
     */
    @Nonnull
    public PlanExportResponseState getState() {
        return state;
    }

    @Nonnull
    @Override
    protected DataMetricCounter getStatusCounter() {
        return PLAN_EXPORT_STATUS_COUNTER;
    }

    /**
     * Update metrics for Action.
     */
    @Override
    protected void completeOperation() {
        getDurationTimer().observe();
        getStatusCounter().labels(getStatus().name()).increment();
    }
}
