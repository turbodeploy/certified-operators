package com.vmturbo.topology.processor.topology.clone;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * The {@link ContainerCloneEditor} implements the clone function for the container.
 */
public class ContainerCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl containerImpl,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        final TopologyEntity.Builder clonedContainer = super.clone(containerImpl, cloneContext,
                                                                   cloneInfo);
        final AnalysisSettingsImpl analysisSettings = clonedContainer.getTopologyEntityImpl()
                .getOrCreateAnalysisSettings();
        if (!cloneContext.isMigrateContainerWorkloadPlan()) {
            // Set controllable to false to avoid generating actions on containers when
            // neither apply constraints, nor migrate container workload plan feature is enabled.
            analysisSettings.setControllable(false);
        }
        // Disable suspend to prevent container from being suspended. Resize is allowed.
        analysisSettings.setSuspendable(false);
        if (cloneContext.isMigrateContainerWorkloadPlan()) {
            // Update the controlledBy relationship
            replaceConnectedEntities(clonedContainer, cloneContext, cloneInfo,
                                     ConnectionType.CONTROLLED_BY_CONNECTION_VALUE);
        }
        return clonedContainer;
    }

    @Override
    protected boolean shouldCopyBoughtCommodity(@Nonnull CommodityBoughtView commodityBought,
                                                @Nonnull CloneContext cloneContext) {
        if (super.shouldCopyBoughtCommodity(commodityBought, cloneContext)) {
            // If the commodity does not have a key, keep it
            return true;
        }
        return cloneContext.shouldApplyConstraints() || cloneContext.isMigrateContainerWorkloadPlan();
    }

    @Override
    protected boolean shouldReplaceBoughtKey(@Nonnull final CommodityTypeView commodityType,
                                             final int providerEntityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && CommodityType.VMPM_ACCESS_VALUE == commodityType.getType()
                && EntityType.CONTAINER_POD_VALUE == providerEntityType;
    }
}
