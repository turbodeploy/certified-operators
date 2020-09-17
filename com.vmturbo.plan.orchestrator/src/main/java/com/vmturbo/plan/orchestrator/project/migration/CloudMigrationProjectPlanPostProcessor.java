package com.vmturbo.plan.orchestrator.project.migration;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.plan.orchestrator.project.PlanProjectStatusTracker;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessorImpl;

/**
 * Performs cloud migration post-processing related tasks.
 */
public class CloudMigrationProjectPlanPostProcessor extends ProjectPlanPostProcessorImpl {
    /**
     * Creates a new migration post-processor.
     *
     * @param instanceId Plan instance id.
     * @param statusTracker Tracker for plan project status.
     */
    public CloudMigrationProjectPlanPostProcessor(final long instanceId,
                                                  @Nonnull final PlanProjectStatusTracker statusTracker) {
        super(instanceId, statusTracker);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean appliesTo(@Nonnull final TopologyInfo sourceTopologyInfo) {
        return sourceTopologyInfo.getPlanInfo().getPlanProjectType()
                == PlanProjectType.CLOUD_MIGRATION;
    }
}
