package com.vmturbo.topology.processor.topology.clone;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * The {@link VirtualMachineCloneEditor} implements the clone function for the virtual machine
 * with special handling to MPC plan.
 */
public class VirtualMachineCloneEditor extends DefaultEntityCloneEditor {

    VirtualMachineCloneEditor(@Nonnull final TopologyInfo topologyInfo,
                              @Nonnull final IdentityProvider identityProvider) {
        super(topologyInfo, identityProvider);
    }

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityDTO.Builder vmDTOBuilder,
                                        final long cloneCounter,
                                        @Nonnull final Map<Long, Builder> topology) {
        final TopologyEntity.Builder clonedVM = super.clone(vmDTOBuilder, cloneCounter, topology);
        // a temporary fix for MPC to work.
        final boolean isCloudMigrationPlan = topologyInfo.hasPlanInfo()
                && PlanProjectType.CLOUD_MIGRATION.name().equals(topologyInfo.getPlanInfo().getPlanType());
        clonedVM.getEntityBuilder().getAnalysisSettingsBuilder().setShopTogether(!isCloudMigrationPlan);
        return clonedVM;
    }
}
