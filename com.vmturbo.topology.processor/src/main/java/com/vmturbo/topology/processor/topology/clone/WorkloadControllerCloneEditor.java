package com.vmturbo.topology.processor.topology.clone;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * The {@link WorkloadControllerCloneEditor} implements the clone function for the workload
 * controller.
 */
public class WorkloadControllerCloneEditor extends DefaultEntityCloneEditor {

    protected void cloneContainerPodsDecreaseReplicas(
        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
        @Nonnull final CloneContext cloneContext,
        @Nonnull final CloneInfo cloneInfo,
        @Nonnull final List<TopologyEntity> sourceEntities,
        final int entityType,
        final int numReplicasToAdd) {
        final DefaultEntityCloneEditor entityCloneEditor =
            EntityCloneEditorFactory.createEntityCloneFunction(entityType);

        AtomicInteger cloneCounter = new AtomicInteger(0);

        // TODO: When decreasing the replica count, we are simply limiting the list of original
        //  entities to the desired number. It may be better to come up with a better mechanism
        //  of choosing which entities to clone, such as ranking the entities, and cloning those
        //  with the highest ranks.
        sourceEntities.stream()
                      .limit(numReplicasToAdd)
                      .map(TopologyEntity::getTopologyEntityImpl)
                      .filter(entity -> entity.getEntityType() == entityType)
                      .forEach(
                          entity -> entityCloneEditor.clone(entity, topologyGraph, cloneContext,
                                                            cloneInfo.withCloneCounter(
                                                                cloneCounter.incrementAndGet())));
    }

    protected Long getOidToClone(@Nonnull final List<TopologyEntity> sourceEntities) {
        // TODO: When increasing the replica count, we are currently choosing the first entity in
        //  the list and cloning it multiple times. In the future, we should come up with some
        //  kind of mechanism for choosing the ideal entity or entities to clone, be that by
        //  ranking them or creating them from scratch, based upon historical data.
        return sourceEntities.stream()
                             .map(TopologyEntity::getTopologyEntityImpl)
                             .map(TopologyEntityImpl::getOid)
                             .findFirst()
                             .orElse(-1L);
    }

    protected void cloneContainerPodsIncreaseReplicas(
        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
        @Nonnull final CloneContext cloneContext,
        @Nonnull final CloneInfo cloneInfo,
        @Nonnull final List<TopologyEntity> sourceEntities,
        final int entityType,
        final int numReplicasToAdd) {
        final DefaultEntityCloneEditor entityCloneEditor =
            EntityCloneEditorFactory.createEntityCloneFunction(entityType);
        final Long oidToClone = this.getOidToClone(sourceEntities);

        AtomicInteger cloneCounter = new AtomicInteger(0);

        sourceEntities.stream()
                      .map(TopologyEntity::getTopologyEntityImpl)
                      .filter(entity -> entity.getEntityType() == entityType)
                      .forEach(entity -> {
                          if (entity.getOid() == oidToClone) {
                              LongStream.range(0, numReplicasToAdd + 1)
                                        .forEach(
                                            cInfo -> entityCloneEditor.clone(entity, topologyGraph,
                                                                             cloneContext,
                                                                             cloneInfo.withCloneCounter(
                                                                                 cloneCounter.incrementAndGet())));
                          } else {
                              entityCloneEditor.clone(entity, topologyGraph, cloneContext,
                                                      cloneInfo.withCloneCounter(
                                                          cloneCounter.incrementAndGet()));
                          }
                      });
    }

    protected void cloneContainerPodEntities(
        @Nonnull final Builder origEntity,
        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
        @Nonnull final CloneContext cloneContext,
        @Nonnull final CloneInfo cloneInfo,
        @Nonnull final Relation relation,
        final int entityType) {
        int originalReplicaCount = origEntity.getConsumers()
                                             .size();
        int updatedReplicaCount = cloneInfo.getChangeReplicas()
                                           .orElse(originalReplicaCount);

        if (updatedReplicaCount == originalReplicaCount) {
            super.cloneRelatedEntities(origEntity, topologyGraph, cloneContext, cloneInfo, relation,
                                       entityType);
        } else if (updatedReplicaCount < originalReplicaCount) {
            this.cloneContainerPodsDecreaseReplicas(topologyGraph, cloneContext, cloneInfo,
                                                    origEntity.getConsumers(), entityType,
                                                    updatedReplicaCount);
        } else {
            int numReplicasToAdd = updatedReplicaCount - originalReplicaCount;
            this.cloneContainerPodsIncreaseReplicas(topologyGraph, cloneContext, cloneInfo,
                                                    origEntity.getConsumers(), entityType,
                                                    numReplicasToAdd);
        }
    }

    @Override
    protected void cloneRelatedEntities(
        @Nonnull final Builder origEntity,
        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
        @Nonnull final CloneContext cloneContext,
        @Nonnull final CloneInfo cloneInfo,
        @Nonnull final Relation relation,
        final int entityType) {
        if (entityType == EntityType.CONTAINER_POD_VALUE && cloneInfo.getChangeReplicas()
                                                                     .isPresent()) {
            cloneContainerPodEntities(
                origEntity, topologyGraph, cloneContext, cloneInfo, relation, entityType);
        } else {
            super.cloneRelatedEntities(origEntity, topologyGraph, cloneContext, cloneInfo, relation,
                                       entityType);
        }
    }

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
