package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import gnu.trove.map.TIntIntMap;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

/**
 * Class for caching the constructed topology from the live topology broadcast pipeline so it can be
 * used by the plan over live topology pipeline.
 */
public class CachedTopology {
    /**
     * Holds a copy of the result from the most recent constructed topology stage run by the live
     * topology broadcast or null if none have run successfully yet.
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
     * @return {@link CachedTopologyResult} for the most recently cached topology.
     */
    public synchronized CachedTopologyResult getTopology() {
        final Stream<TopologyEntity.Builder> entities = cachedMap.values().stream();
        final TIntIntMap removedCounts = null;
        return new CachedTopologyResult(removedCounts,
                entities.collect(Collectors.toMap(TopologyEntity.Builder::getOid, TopologyEntity.Builder::snapshot)));
    }

    /**
     * Obtain specific entities from the cached topology.
     *
     * <p>Requested entities are returned in the form of {@link TopologyEntityDTO} objects. If any
     * requested entities are not present in the cached topology, they are silently omitted from the
     * results, so caller should check if it matters.</p>
     *
     * @param entities oids of requested entities
     * @return map of oids to retrieved entities; missing entities are omitted from the map
     */
    public synchronized Map<Long, TopologyEntityDTO> getCachedEntitiesAsTopologyEntityDTOs(List<Long> entities) {
        return entities.stream()
                .filter(cachedMap::containsKey)
                .collect(Collectors.toMap(Function.identity(),
                        id -> cachedMap.get(id).build().getTopologyEntityDtoBuilder().build()));
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
