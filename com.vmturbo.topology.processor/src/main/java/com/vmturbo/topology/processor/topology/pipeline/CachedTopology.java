package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * Class for caching the constructed topology from the live topology broadcast pipeline so it
 * can be used by the plan over live topology pipeline.
 */
public class CachedTopology {
    /**
     * Holds a copy of the result from the most recent constructed topology stage run by the
     * live topology broadcast or null if none have run successfully yet.
     */
    private Map<Long, TopologyEntity.Builder> cachedMap;

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
     * @return the most recently cached topology.
     */
    public synchronized Map<Long, TopologyEntity.Builder> getTopology() {
        return cachedMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().snapshot()));
    }

    /**
     * Return true if no topology has been cached yet.
     *
     * @return boolean indicating if the cache is empty or not.
     */
    public synchronized boolean isEmpty() {
        return cachedMap == null || cachedMap.isEmpty();
    }
}
