package com.vmturbo.plan.orchestrator.project.migration;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.plan.orchestrator.project.PlanProjectStatusTracker;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessorImpl;

/**
 * Performs migration plan post-processing related tasks.
 */
public class MigrationProjectPlanPostProcessor extends ProjectPlanPostProcessorImpl {
    /**
     * Creates a new migration post-processor.
     *
     * @param instanceId Plan instance id.
     * @param statusTracker Tracker for plan project status.
     */
    public MigrationProjectPlanPostProcessor(final long instanceId,
                                                  @Nonnull final PlanProjectStatusTracker statusTracker) {
        super(instanceId, statusTracker);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo sourceTopologyInfo) {
        final PlanProjectType planProjectType = sourceTopologyInfo.getPlanInfo().getPlanProjectType();
        return PlanProjectType.CLOUD_MIGRATION == planProjectType
                || PlanProjectType.CONTAINER_MIGRATION == planProjectType;
    }
}
