package com.vmturbo.topology.processor.topology.clone;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * The {@link ContainerPodCloneEditor} implements the clone function for the container pod and all
 * containers running in the pod.
 */
public class ContainerPodCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl podImpl,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        final TopologyEntity.Builder origPod = cloneContext.getTopology().get(podImpl.getOid());
        // Clone myself
        final TopologyEntity.Builder clonedPod = super.clone(podImpl, cloneContext, cloneInfo);
        // Clone consumer containers
        cloneRelatedEntities(origPod, cloneContext, cloneInfo, Relation.Consumer,
                             EntityType.CONTAINER_VALUE);
        // Update aggregatedBy relationship for workload migration plan
        if (cloneContext.isMigrateContainerWorkloadPlan()) {
            replaceConnectedEntities(clonedPod, cloneContext, cloneInfo,
                                     ConnectionType.AGGREGATED_BY_CONNECTION_VALUE);
        }
        return clonedPod;
    }

    @Override
    protected boolean shouldSkipProvider(
            @Nonnull CommoditiesBoughtFromProviderView boughtFromProvider,
            @Nonnull final CloneContext cloneContext) {
        // As we aren't copying the related workload controller nor the volume into the plan, we
        // will skip those providers.
        return Objects.requireNonNull(boughtFromProvider).hasProviderEntityType()
                && (boughtFromProvider.getProviderEntityType() == EntityType.VIRTUAL_VOLUME_VALUE
                || (!cloneContext.isMigrateContainerWorkloadPlan()
                && boughtFromProvider.getProviderEntityType() == EntityType.WORKLOAD_CONTROLLER_VALUE));
    }

    @Override
    protected boolean shouldCopyBoughtCommodity(@Nonnull CommodityBoughtView commodityBought,
                                                @Nonnull final CloneContext cloneContext) {
        if (super.shouldCopyBoughtCommodity(commodityBought, cloneContext)) {
            // If the commodity does not have a key, keep it
            return true;
        }
        if (cloneContext.shouldApplyConstraints() || cloneContext.isMigrateContainerWorkloadPlan()) {
            final Map<CommodityType, Set<String>> nodeCommodities =
                    cloneContext.getNodeCommodities();
            // When feature flag is on and this is a container cluster plan
            switch (commodityBought.getCommodityType().getType()) {
                // We drop the cluster commodity, because there is no need for such a restriction
                // for pods in the container cluster plan where there's only one container cluster.
                // In the future when we support multiple clusters in plan, we will revisit but this
                // still seems a good choice not to restrict the pods in any particular cluster.
                case CommodityType.CLUSTER_VALUE:
                    return false;
                // For taint commodity, we only drop it when the taint does not exist in the cluster
                // of the plan
                case CommodityType.TAINT_VALUE:
                    return nodeCommodities.containsKey(CommodityType.TAINT)
                            && nodeCommodities.get(CommodityType.TAINT)
                            .contains(commodityBought.getCommodityType().getKey());
                // For label commodity, we only drop it when the label does not exist in the cluster
                // of the plan
                case CommodityType.LABEL_VALUE:
                    return nodeCommodities.containsKey(CommodityType.LABEL)
                            && nodeCommodities.get(CommodityType.LABEL)
                            .contains(commodityBought.getCommodityType().getKey());
                // For all other commodities with key, keep them
                default:
                    return true;
            }
        }
        // When feature flag is disabled, drop the commodity
        return false;
    }

    @Override
    protected boolean shouldReplaceBoughtKey(@Nonnull final CommodityTypeView commodityType,
                                             final int providerEntityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && TopologyEditorUtil.isQuotaCommodity(commodityType.getType())
                && EntityType.WORKLOAD_CONTROLLER_VALUE == providerEntityType;
    }

    @Override
    protected boolean shouldReplaceSoldKey(@Nonnull final CommodityTypeView commodityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && CommodityType.VMPM_ACCESS_VALUE == commodityType.getType();
    }
}
