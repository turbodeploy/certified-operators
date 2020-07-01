package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

/**
 * Class for caching the constructed topology from the live topology broadcast pipeline so it
 * can be used by the plan over live topology pipeline.
 */
public class CachedTopology {
    /**
     * Holds a copy of the result from the most recent constructed topology stage run by the
     * live topology broadcast or null if none have run successfully yet.
     */
    private Map<Long, TopologyEntity.Builder> cachedMap = Collections.emptyMap();

    /**
     * Cache the result of the construct topology stage.
     *
     * @param newMap Topology map to be cached.
     */
    synchronized void updateTopology(@Nonnull Map<Long, TopologyEntity.Builder> newMap) {
        cachedMap = newMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
    }

    /**
     * Return a deep copy of the cached topology.
     *
     * @param pendingPlanType Information about the topology under construction. Certain topology
     *                        types (e.g. reservation plan) may get different subsets of entities.
     * @return {@link CachedTopologyResult} for the most recently cached topology.
     */
    public synchronized CachedTopologyResult getTopology(
            @Nullable final PlanProjectType pendingPlanType) {
        final Stream<TopologyEntity.Builder> entities;
        final TIntIntMap removedCounts;
        if (pendingPlanType == PlanProjectType.RESERVATION_PLAN) {
            removedCounts = new TIntIntHashMap();
            entities = cachedMap.values().stream()
                .filter(entity -> {
                    // All entities above vm in supply chain should be removed for reservation plan.
                    // This helps run faster, because we don't need to snapshot all these entities,
                    // which can be hundreds of thousands in large environment.
                    if (entity.getEntityType() == EntityType.APPLICATION_SERVER.getValue() ||
                            entity.getEntityType() == EntityType.DATABASE_SERVER.getValue() ||
                            entity.getEntityType() == EntityType.BUSINESS_APPLICATION.getValue() ||
                            entity.getEntityType() == EntityType.APPLICATION.getValue() ||
                            entity.getEntityType() == EntityType.CONTAINER.getValue() ||
                            entity.getEntityType() == EntityType.CONTAINER_POD.getValue() ||
                            entity.getEntityType() == EntityType.VIRTUAL_VOLUME.getValue()) {
                        removedCounts.adjustOrPutValue(entity.getEntityType(), 1, 1);
                        return false;
                    } else {
                        return true;
                    }
                });
        } else {
            removedCounts = null;
            entities = cachedMap.values().stream();
        }
        return new CachedTopologyResult(removedCounts,
            entities.collect(Collectors.toMap(TopologyEntity.Builder::getOid, TopologyEntity.Builder::snapshot)));
    }

    /**
     * Return true if no topology has been cached yet.
     *
     * @return boolean indicating if the cache is empty or not.
     */
    public synchronized boolean isEmpty() {
        return cachedMap.isEmpty();
    }

    /**
     * Return object for {@link CachedTopology#getTopology(PlanProjectType)}, containing additional
     * information about the cached topology which is useful for topology pipeline sumaries.
     */
    public static class CachedTopologyResult {
        private final TIntIntMap removedCounts;

        private final Map<Long, TopologyEntity.Builder> entities;

        CachedTopologyResult(@Nullable final TIntIntMap removedCounts,
                             @Nonnull final Map<Long, Builder> entities) {
            this.removedCounts = removedCounts;
            this.entities = entities;
        }

        @Nonnull
        public Map<Long, TopologyEntity.Builder> getEntities() {
            return entities;
        }

        @Override
        public String toString() {
            StringBuilder summary = new StringBuilder();
            summary.append("Using cached topology of size ").append(entities.size());
            if (removedCounts != null) {
                summary.append("\n")
                    .append("Removed entities:\n");
                removedCounts.forEachEntry((type, amount) -> {
                    summary.append(ApiEntityType.fromType(type)).append(":").append(amount).append("\n");
                    return true;
                });
            }
            return summary.toString();
        }
    }
}
