package com.vmturbo.topology.processor.topology.clone;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.stitching.TopologyEntity;

/**
 * The {@link VirtualMachineCloneEditor} implements the clone function for the virtual machine
 * with special handling to MPC plan.
 */
public class VirtualMachineCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl vmImpl,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        final TopologyEntity.Builder clonedVM = super.clone(vmImpl, cloneContext, cloneInfo);
        // a temporary fix for MPC to work.
        final boolean isCloudMigrationPlan =
                PlanProjectType.CLOUD_MIGRATION.name().equals(cloneContext.getPlanType());
        clonedVM.getTopologyEntityImpl().getOrCreateAnalysisSettings().setShopTogether(!isCloudMigrationPlan);
        return clonedVM;
    }
}
