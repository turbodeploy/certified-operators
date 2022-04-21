package com.vmturbo.topology.processor.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationView;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * A utility class that contains helper functions for topology entity clone.
 */
public final class TopologyEditorUtil {

    private static final Set<Integer> quotaCommodities = ImmutableSet.of(
            CommodityType.VCPU_LIMIT_QUOTA_VALUE,
            CommodityType.VCPU_REQUEST_QUOTA_VALUE,
            CommodityType.VMEM_LIMIT_QUOTA_VALUE,
            CommodityType.VMEM_REQUEST_QUOTA_VALUE
    );

    private TopologyEditorUtil() {}

    /**
     * Recursively looking for the consumers or providers of a type from the given entity.
     *
     * @param targetType the type of entity to be found
     * @param entity the given entity candidate
     * @param topologyGraph the topology graph
     * @param traverseUp traverse up or down the supply chain to find related entities
     * @return a set of TopologyEntity.Builder
     */
    public static Set<Builder> traverseSupplyChain(
            final int targetType,
            final @Nonnull TopologyEntity.Builder entity,
            final @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
            final boolean traverseUp) {
        Set<TopologyEntity.Builder> targetEntities = new HashSet<>();
        final Set<TopologyEntity> directRelatedEntities;
        if (traverseUp) {
            directRelatedEntities = topologyGraph.getEntity(entity.getOid())
                    .get().getConsumers().stream()
                    .collect(Collectors.toSet());
        } else {
            directRelatedEntities = topologyGraph.getEntity(entity.getOid())
                    .get().getProviders().stream()
                    .collect(Collectors.toSet());
        }
        if (directRelatedEntities.isEmpty()) {
            return targetEntities;
        }
        targetEntities = directRelatedEntities.stream()
                .filter(e -> targetType == e.getEntityType())
                .map(TopologyEntity::getTopologyEntityImpl)
                .map(TopologyEntity::newBuilder)
                .collect(Collectors.toSet());
        if (targetEntities.isEmpty()) {
            for (TopologyEntity c : directRelatedEntities) {
                targetEntities.addAll(
                        traverseSupplyChain(targetType,
                                            TopologyEntity.newBuilder(c.getTopologyEntityImpl()),
                                            topologyGraph,
                                            traverseUp));
            }
        }
        return targetEntities;
    }

    /**
     * Get the container cluster entity builder from plan scope.
     *
     * @return the container cluster entity builder
     */
    @Nonnull
    public static Optional<Builder> getContainerCluster(@Nonnull final TopologyInfo topologyInfo,
                                                        @Nullable final PlanScope scope,
                                                        @Nonnull final Map<Long, Builder> topology) {
        final String planType = topologyInfo.getPlanInfo().getPlanType();
        if (!StringConstants.MIGRATE_CONTAINER_WORKLOAD_PLAN.equals(planType)
                && !StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN.equals(planType)) {
            return Optional.empty();
        }
        return Optional.ofNullable(scope)
                .map(PlanScope::getScopeEntriesList)
                .flatMap(scopeEntries -> scopeEntries.stream()
                        .filter(scopeEntry -> scopeEntry.getClassName()
                                .equals(ApiEntityType.CONTAINER_PLATFORM_CLUSTER.apiStr()))
                        .findAny())
                .map(PlanScopeEntry::getScopeObjectOid)
                .map(topology::get);
    }

    /**
     * Get the container cluster vendor ID.
     *
     * @param containerCluster the container cluster entity
     * @return the container cluster vendor ID
     */
    @Nonnull
    public static Optional<String> getContainerClusterVendorId(
            @Nonnull TopologyEntity.Builder containerCluster) {
        return containerCluster
                .getTopologyEntityImpl()
                .getOrigin()
                .getDiscoveryOrigin()
                .getDiscoveredTargetDataMap()
                .values().stream()
                .filter(PerTargetEntityInformationView::hasVendorId)
                .map(PerTargetEntityInformationView::getVendorId)
                .findFirst();
    }

    /**
     * Return if a commodity is a quota commodity.
     *
     * @param commodityType the commodity type
     * @return true if a commodity is a quota commodity
     */
    public static boolean isQuotaCommodity(final int commodityType) {
        return quotaCommodities.contains(commodityType);
    }
}
