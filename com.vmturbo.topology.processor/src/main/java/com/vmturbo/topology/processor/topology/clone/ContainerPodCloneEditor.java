package com.vmturbo.topology.processor.topology.clone;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * The {@link ContainerPodCloneEditor} implements the clone function for the container pod and all
 * containers running in the pod.
 */
public class ContainerPodCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl podImpl,
                                        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        final TopologyEntity.Builder origPod = cloneContext.getTopology().get(podImpl.getOid());
        // Clone myself
        final TopologyEntity.Builder clonedPod = super.clone(podImpl, topologyGraph,
                                                             cloneContext, cloneInfo);
        // Clone consumer containers
        cloneRelatedEntities(origPod, topologyGraph, cloneContext, cloneInfo, Relation.Consumer,
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
                                                @Nonnull final CloneContext cloneContext,
                                                @Nonnull final TopologyEntityView entity) {
        if (super.shouldCopyBoughtCommodity(commodityBought, cloneContext, entity)) {
            // If the commodity does not have a key, keep it
            return true;
        }
        if (entity.getAnalysisSettings() != null && entity.getAnalysisSettings().getDaemon()) {
            // Ensure daemon pods will always be placed.  This is a workaround while waiting for
            // the ultimate solution for the market analysis to handle initial placement (in plan)
            // for daemons.
            return false;
        }
        if (cloneContext.shouldApplyConstraints() || cloneContext.isMigrateContainerWorkloadPlan()) {
            final Map<CommodityType, Set<String>> nodeCommodities =
                    cloneContext.getNodeCommodities();
            // When feature flag is on and this is a container cluster plan
            switch (commodityBought.getCommodityType().getType()) {
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
        if (!Objects.requireNonNull(commodityType).hasKey()) {
            return false;
        }
        return (TopologyEditorUtil.isQuotaCommodity(commodityType.getType())
                    && EntityType.WORKLOAD_CONTROLLER_VALUE == providerEntityType)
                || ((commodityType.getType() == CommodityType.CLUSTER_VALUE)
                    && (EntityType.VIRTUAL_MACHINE_VALUE == providerEntityType));
    }

    @Override
    protected CommodityTypeView getReplacedBoughtCommodity(@Nonnull final CommodityTypeView commodityType,
                                                           @Nonnull final CloneContext cloneContext,
                                                           @Nonnull final CloneInfo cloneInfo) {
        if (commodityType.getType() == CommodityType.CLUSTER_VALUE) {
            return cloneContext.getPlanClusterVendorId()
                    .map(id -> commodityType.copy().setKey(id))
                    .orElseThrow(() -> new TopologyEditorException(
                            "Failed to get cluster ID for plan " + cloneContext.getPlanId()));
        }
        return super.getReplacedBoughtCommodity(commodityType, cloneContext, cloneInfo);
    }

    @Override
    protected boolean shouldReplaceSoldKey(@Nonnull final CommodityTypeView commodityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && CommodityType.VMPM_ACCESS_VALUE == commodityType.getType();
    }

    @Override
    protected void updateAnalysisSettings(@Nonnull final TopologyEntity.Builder clonedPod,
                                          @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                          @Nonnull final CloneContext cloneContext) {
        final AnalysisSettingsImpl analysisSettings =
                clonedPod.getTopologyEntityImpl().getOrCreateAnalysisSettings();
        if (!analysisSettings.hasConsistentScalingFactor()) {
            TopologyEditorUtil.computeConsistentScalingFactor(clonedPod)
                    .ifPresent(analysisSettings::setConsistentScalingFactor);
        }
    }

    @Override
    protected String getCloneDisplayName(
        @Nonnull final TopologyEntityImpl entityImpl,
        @Nonnull final CloneInfo cloneInfo) {
        return cloneInfo.getSourceCluster()
                        .map(TopologyEntity.Builder::getDisplayName)
                        .map(clonedFrom -> {
                            return String.format("%s - Clone #%d from %s",
                                                 entityImpl.getDisplayName().substring(0, entityImpl.getDisplayName().lastIndexOf("-")),
                                                 cloneInfo.getCloneCounter(), clonedFrom);
                        })
                        .orElse(
                            entityImpl.getDisplayName() + cloneSuffix(cloneInfo.getCloneCounter()));
    }

    @Override
    protected Long getProviderId(@Nonnull final CloneContext cloneContext,
                                 @Nonnull final CloneInfo cloneInfo,
                                 final long origProviderId) {
        Long providerId = cloneContext.getClonedEntityId(cloneInfo.getCloneCounter(), origProviderId);
        // When increasing the number of replicas for a WorkloadController as part
        // of a Workload Migration Plan, the associated workload controller will
        // only be cloned a single time, , so we need to specify a cloneCounter of 0 to
        // locate the cloned workloadController.
        if (providerId == null && cloneInfo.getCloneCounter() > 0) {
            providerId = cloneContext.getClonedEntityId(0, origProviderId);
        }
        return providerId;
    }
}
