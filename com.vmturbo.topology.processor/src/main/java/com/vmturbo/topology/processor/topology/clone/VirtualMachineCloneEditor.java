package com.vmturbo.topology.processor.topology.clone;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * The {@link VirtualMachineCloneEditor} implements the clone function for the virtual machine
 * with special handling to MPC plan.
 */
public class VirtualMachineCloneEditor extends DefaultEntityCloneEditor {

    @Override
    protected void updateAnalysisSettings(@Nonnull final TopologyEntity.Builder clonedVM,
                                          @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                          @Nonnull final CloneContext cloneContext) {
        // a temporary fix for MPC to work.
        final boolean isCloudMigrationPlan =
                PlanProjectType.CLOUD_MIGRATION.name().equals(cloneContext.getPlanType());
        clonedVM.getTopologyEntityImpl().getOrCreateAnalysisSettings().setShopTogether(!isCloudMigrationPlan);
    }
}
