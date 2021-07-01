package com.vmturbo.topology.processor.targets.status;

import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;

/**
 * Contains information describes target status.
 */
@Immutable
public class TargetStatusPojo {
    private final long targetId;
    private final List<ProbeStageDetails> stagesReports;
    private final LocalDateTime operationCompletionTime;

    /**
     * Constructor.
     *
     * @param targetId the target id
     * @param stagesReports the stage reports of the discovery/validation
     * @param operationCompletionTime the completion time of the discovery/validation
     */
    public TargetStatusPojo(final long targetId, @Nonnull final List<ProbeStageDetails> stagesReports,
            @Nonnull LocalDateTime operationCompletionTime) {
        this.targetId = targetId;
        this.stagesReports = stagesReports;
        this.operationCompletionTime = operationCompletionTime;
    }

    public long getTargetId() {
        return targetId;
    }

    @Nonnull
    public List<ProbeStageDetails> getStagesReports() {
        return stagesReports;
    }

    @Nonnull
    public LocalDateTime getOperationCompletionTime() {
        return operationCompletionTime;
    }
}
