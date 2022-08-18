package com.vmturbo.topology.processor.topology.clone;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.WorkloadControllerInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoView;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.topology.TopologyEditorException;
import com.vmturbo.topology.processor.util.TopologyEditorUtil;

/**
 * The {@link WorkloadControllerCloneEditor} implements the clone function for the workload
 * controller.
 */
public class WorkloadControllerCloneEditor extends DefaultEntityCloneEditor {
    private static final String DAEMON_POD_GROUP_DESCRIPTION_PREFIX = "Daemon pod group ";

    private Collection<TopologyEntity.Builder> cloneContainerPodsDecreaseReplicas(
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
        return sourceEntities.stream()
                      .limit(numReplicasToAdd)
                      .map(TopologyEntity::getTopologyEntityImpl)
                      .filter(entity -> entity.getEntityType() == entityType)
                      .map(
                          entity -> entityCloneEditor.clone(entity, topologyGraph, cloneContext,
                                                            cloneInfo.withCloneCounter(
                                                                cloneCounter.incrementAndGet())))
                      .collect(Collectors.toList());
    }

    private Long getOidToClone(@Nonnull final List<TopologyEntity> sourceEntities) {
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

    private Collection<TopologyEntity.Builder> cloneContainerPodsIncreaseReplicas(
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

        return sourceEntities.stream()
                      .map(TopologyEntity::getTopologyEntityImpl)
                      .filter(entity -> entity.getEntityType() == entityType)
                      .flatMap(entity -> {
                          if (entity.getOid() == oidToClone) {
                              return LongStream.range(0, numReplicasToAdd + 1)
                                        .mapToObj(
                                            cInfo -> entityCloneEditor.clone(entity, topologyGraph,
                                                                             cloneContext,
                                                                             cloneInfo.withCloneCounter(
                                                                                 cloneCounter.incrementAndGet())));
                          } else {
                              return Stream.of(entityCloneEditor.clone(entity, topologyGraph, cloneContext,
                                                      cloneInfo.withCloneCounter(
                                                          cloneCounter.incrementAndGet())));
                          }
                      }).collect(Collectors.toList());
    }

    private Collection<TopologyEntity.Builder> cloneContainerPodEntities(
        @Nonnull final Builder origEntity,
        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
        @Nonnull final CloneContext cloneContext,
        @Nonnull final CloneInfo cloneInfo,
        @Nonnull final Relation relation,
        final int entityType) {
        int originalReplicaCount = origEntity.getConsumers().size();
        int updatedReplicaCount = isDaemonSet(origEntity) ? cloneContext.getPlanClusterNodeCount()
                : cloneInfo.getChangeReplicas().orElse(originalReplicaCount);

        final Collection<TopologyEntity.Builder> clonedPods;
        if (updatedReplicaCount == originalReplicaCount) {
            clonedPods = super.cloneRelatedEntities(origEntity, topologyGraph, cloneContext, cloneInfo,
                    relation, entityType);
        } else if (updatedReplicaCount < originalReplicaCount) {
            clonedPods = this.cloneContainerPodsDecreaseReplicas(topologyGraph, cloneContext,
                    cloneInfo, origEntity.getConsumers(), entityType, updatedReplicaCount);
        } else {
            int numReplicasToAdd = updatedReplicaCount - originalReplicaCount;
            clonedPods = this.cloneContainerPodsIncreaseReplicas(topologyGraph, cloneContext,
                    cloneInfo, origEntity.getConsumers(), entityType, numReplicasToAdd);
        }

        if (isDaemonSet(origEntity)) {
            final Set<Long> clonedPodIds = clonedPods.stream()
                    .map(TopologyEntity.Builder::getOid).collect(Collectors.toSet());
            final Grouping daemonPodGroup = PolicyManager.generateStaticGroup(
                    clonedPodIds, EntityType.CONTAINER_POD_VALUE,
                    DAEMON_POD_GROUP_DESCRIPTION_PREFIX + origEntity.getDisplayName());
            final Grouping nodeGroup = cloneContext.getPlanClusterProviderNodeGroup();
            if (nodeGroup != null) {
                cloneContext.addPlacementPolicy(daemonPodGroup, cloneContext.getPlanClusterProviderNodeGroup(), 1);
            }
        }
        return clonedPods;
    }

    @Override
    protected Collection<Builder> cloneRelatedEntities(
        @Nonnull final Builder origEntity,
        @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
        @Nonnull final CloneContext cloneContext,
        @Nonnull final CloneInfo cloneInfo,
        @Nonnull final Relation relation,
        final int entityType) {
        if (entityType == EntityType.CONTAINER_POD_VALUE && cloneInfo.getChangeReplicas()
                                                                     .isPresent()) {
            return cloneContainerPodEntities(
                origEntity, topologyGraph, cloneContext, cloneInfo, relation, entityType);
        } else {
            return super.cloneRelatedEntities(origEntity, topologyGraph, cloneContext, cloneInfo, relation,
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
        @Nonnull CloneContext cloneContext, @Nonnull TopologyEntityView entity) {
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

    private static boolean isDaemonSet(@Nullable final TopologyEntity.Builder entity) {
        return Optional.ofNullable(entity)
                .map(e -> e.getTopologyEntityImpl().getTypeSpecificInfo())
                .map(TypeSpecificInfoView::getWorkloadController)
                .map(WorkloadControllerInfoView::hasDaemonSetInfo)
                .orElse(false);
    }
}
