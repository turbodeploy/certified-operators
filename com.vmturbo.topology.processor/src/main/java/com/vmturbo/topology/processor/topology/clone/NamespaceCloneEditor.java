package com.vmturbo.topology.processor.topology.clone;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * The {@link NamespaceCloneEditor} implements the clone function for the namespace.
 */
public class NamespaceCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl nsImpl,
                                        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                        @Nonnull final CloneContext cloneContext,
                                        @Nonnull final CloneInfo cloneInfo) {
        // Clone myself
        final TopologyEntity.Builder clonedNS = super.clone(nsImpl, topologyGraph,
                                                            cloneContext, cloneInfo);
        // Update aggregatedBy entities
        replaceAggregatedByEntities(clonedNS.getTopologyEntityImpl(), cloneContext);
        return clonedNS;
    }

    @Override
    @Nullable
    protected Long getProviderId(@Nonnull final CloneContext cloneContext,
                                 @Nonnull final CloneInfo cloneInfo,
                                 final long origProviderId) {
        // For provider of the cloned namespace, we set it to the destination cluster
        return cloneContext.getPlanCluster()
                .map(TopologyEntity.Builder::getOid)
                .orElseThrow(() -> new TopologyEditorException(
                        "Failed to get destination cluster for plan " + cloneContext.getPlanId()));
    }

    @Override
    protected boolean shouldCopyBoughtCommodity(@Nonnull CommodityBoughtView commodityBought,
                                                @Nonnull CloneContext cloneContext,
                                                @Nonnull TopologyEntityView entity) {
        if (super.shouldCopyBoughtCommodity(commodityBought, cloneContext, entity)) {
            // If the commodity does not have a key, keep it
            return true;
        }
        // We drop the cluster commodity, because there is no need for such a restriction
        // for namespaces in the container cluster plan where there's only one container cluster.
        // In the future when we support multiple clusters in plan, we will revisit.
        return commodityBought.getCommodityType().getType() != CommodityType.CLUSTER_VALUE;
    }

    @Override
    protected boolean shouldReplaceSoldKey(@Nonnull final CommodityTypeView commodityType) {
        return Objects.requireNonNull(commodityType).hasKey()
                && TopologyEditorUtil.isQuotaCommodity(commodityType.getType());
    }

    @Override
    protected void updateAnalysisSettings(@Nonnull final TopologyEntity.Builder clonedNS,
                                          @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                          @Nonnull final CloneContext cloneContext) {
        final AnalysisSettingsImpl analysisSettings =
                clonedNS.getTopologyEntityImpl().getOrCreateAnalysisSettings();
        if (!analysisSettings.hasConsistentScalingFactor()) {
            TopologyEditorUtil.computeConsistentScalingFactor(clonedNS)
                    .ifPresent(analysisSettings::setConsistentScalingFactor);
        }
    }

    private void replaceAggregatedByEntities(@Nonnull final TopologyEntityImpl clonedNSImpl,
                                             @Nonnull final CloneContext cloneContext) {
        cloneContext.getPlanCluster()
                .map(TopologyEntity.Builder::getOid)
                .ifPresent(clusterId -> clonedNSImpl.getConnectedEntityListImplList()
                        .forEach(connectedEntityImpl -> {
                            if (connectedEntityImpl.getConnectedEntityType()
                                    == ConnectionType.AGGREGATED_BY_CONNECTION_VALUE) {
                                connectedEntityImpl.setConnectedEntityId(clusterId);
                            }
                        }));
    }
}
