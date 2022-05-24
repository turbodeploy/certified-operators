package com.vmturbo.topology.processor.topology.clone;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * The {@link WorkloadControllerCloneEditor} implements the clone function for the workload controller.
 */
public class WorkloadControllerCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl wcImpl,
                                        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        if (!cloneContext.isMigrateContainerWorkloadPlan()) {
            throw new TopologyEditorException("Cloning workload controller in plan type "
                                                      + cloneContext.getPlanType()
                                                      + " is not supported");
        }
        final TopologyEntity.Builder origWC = cloneContext.getTopology().get(wcImpl.getOid());
        // Clone provider namespace
        cloneRelatedEntities(origWC, topologyGraph, cloneContext, cloneInfo, Relation.Provider,
                             EntityType.NAMESPACE_VALUE);
        // Clone myself (the workload controller)
        final TopologyEntity.Builder clonedWC = super.clone(wcImpl, topologyGraph, cloneContext, cloneInfo);
        // Update aggregatedBy relationship to replace the aggregator with the cloned namespace
        replaceConnectedEntities(clonedWC, cloneContext, cloneInfo,
                                 ConnectionType.AGGREGATED_BY_CONNECTION_VALUE);
        // Clone owned containerSpec
        cloneRelatedEntities(origWC, topologyGraph, cloneContext, cloneInfo, Relation.Owned,
                             EntityType.CONTAINER_SPEC_VALUE);
        // Update owns relationship to replace the owned entities with the cloned containerSpecs
        replaceConnectedEntities(clonedWC, cloneContext, cloneInfo,
                                 ConnectionType.OWNS_CONNECTION_VALUE);
        // Clone consumer pods
        // TODO: clone workload controllers with changing number of replicas
        cloneRelatedEntities(origWC, topologyGraph, cloneContext, cloneInfo, Relation.Consumer,
                             EntityType.CONTAINER_POD_VALUE);
        return clonedWC;
    }

    @Override
    protected boolean shouldCopyBoughtCommodity(@Nonnull CommodityBoughtView commodityBought,
                                                @Nonnull CloneContext cloneContext) {
        // Always copy bought commodities of workload controller
        return true;
    }

    @Override
    protected boolean shouldReplaceBoughtKey(@Nonnull final CommodityTypeView commodityType,
                                             final int providerEntityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && TopologyEditorUtil.isQuotaCommodity(commodityType.getType())
                && EntityType.NAMESPACE_VALUE == providerEntityType;
    }

    @Override
    protected boolean shouldReplaceSoldKey(@Nonnull final CommodityTypeView commodityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && TopologyEditorUtil.isQuotaCommodity(commodityType.getType());
    }

    @Override
    protected void updateAnalysisSettings(@Nonnull final TopologyEntity.Builder clonedWC,
                                          @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                          @Nonnull final CloneContext cloneContext) {
        final AnalysisSettingsImpl analysisSettings =
                clonedWC.getTopologyEntityImpl().getOrCreateAnalysisSettings();
        if (!analysisSettings.hasConsistentScalingFactor()) {
            TopologyEditorUtil.computeConsistentScalingFactor(clonedWC)
                    .ifPresent(analysisSettings::setConsistentScalingFactor);
        }
    }
}
