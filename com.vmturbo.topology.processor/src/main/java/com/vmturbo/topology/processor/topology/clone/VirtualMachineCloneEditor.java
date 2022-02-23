package com.vmturbo.topology.processor.topology.clone;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * The {@link VirtualMachineCloneEditor} implements the clone function for the virtual machine
 * with special handling to MPC plan.
 */
public class VirtualMachineCloneEditor extends DefaultEntityCloneEditor {

    VirtualMachineCloneEditor(@Nonnull final TopologyInfo topologyInfo,
                              @Nonnull final IdentityProvider identityProvider,
                              @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                              @Nullable final PlanScope scope) {
        super(topologyInfo, identityProvider, topology, scope);
    }

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl vmImpl,
                                        final long cloneCounter) {
        final TopologyEntity.Builder clonedVM = super.clone(vmImpl, cloneCounter);
        // a temporary fix for MPC to work.
        final boolean isCloudMigrationPlan = topologyInfo.hasPlanInfo()
                && PlanProjectType.CLOUD_MIGRATION.name().equals(topologyInfo.getPlanInfo().getPlanType());
        clonedVM.getTopologyEntityImpl().getOrCreateAnalysisSettings().setShopTogether(!isCloudMigrationPlan);
        return clonedVM;
    }
}
