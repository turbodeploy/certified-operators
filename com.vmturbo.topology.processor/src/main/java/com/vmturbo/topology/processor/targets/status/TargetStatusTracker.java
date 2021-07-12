package com.vmturbo.topology.processor.targets.status;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.topology.processor.operation.OperationListener;
import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

/**
 * Interface for tracking targets statuses.
 */
public interface TargetStatusTracker extends OperationListener, StringDiagnosable {

    /**
     * Get statuses for the requested targets. If input target ids is null then return statuses for
     * all existed targets.
     *
     * @param targetIds target ids.
     * @param returnAll If true, return all if the target ids collection is empty.
     * @return the map of targets statuses
     */
    @Nonnull
    Map<Long, TargetStatus> getTargetsStatuses(@Nonnull Set<Long> targetIds, boolean returnAll);

    /**
     * Get information about target with failed discoveries.
     *
     * @return the map of failed discoveries for targets
     */
    Map<Long, DiscoveryFailure> getFailedDiscoveries();

    /**
     * Get last successful discovery time of target.
     *
     * @param targetId target id.
     * @return time of last successful discovery.
     */
    @Nullable
    Long getLastSuccessfulDiscoveryTime(long targetId);
}
