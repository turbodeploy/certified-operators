package com.vmturbo.topology.processor.topology.pipeline;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.InvalidProtocolBufferException;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;

/**
 * Class for caching the constructed topology from the live topology broadcast pipeline so it can be
 * used by the plan over live topology pipeline.
 */
public class CachedTopology {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Holds a copy of the result from the most recent constructed topology stage run by the live
     * topology broadcast or null if none have run successfully yet.
     */
    private final CachedEntities<?> cachedEntities;

    /**
     * Create a new {@link CachedTopology}.
     *
     * @param serializeCachedTopology If true, serialize the cached entities to save space. This adds
     *        some time to the {@link CachedTopology#updateTopology(Map)} method.
     */
    public CachedTopology(boolean serializeCachedTopology) {
        if (serializeCachedTopology) {
            cachedEntities = new SerializedCachedEntities();
        } else {
            cachedEntities = new BuilderCachedEntities();
        }
    }

    /**
     * Cache the result of the construct topology stage.
     *
     * @param newMap Topology map to be cached.
     */
    synchronized void updateTopology(@Nonnull Map<Long, TopologyEntityDTO.Builder> newMap) {
        cachedEntities.repopulate(newMap);
    }

    /**
     * Return a deep copy of the cached topology.
     *
     * @return {@link CachedTopologyResult} for the most recently cached topology.
     */
    public synchronized CachedTopologyResult getTopology() {
        final Stream<TopologyEntityDTO.Builder> entities = cachedEntities.getAllEntityBuilders();
        return new CachedTopologyResult(entities.collect(
                Collectors.toMap(TopologyEntityDTO.Builder::getOid, Function.identity())));
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
        return cachedEntities.getEntities(entities)
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
    }

    /**
     * Return true if no topology has been cached yet.
     *
     * @return boolean indicating if the cache is empty or not.
     */
    public synchronized boolean isEmpty() {
        return cachedEntities.isEmpty();
    }

    /**
     * Return object for {@link CachedTopology#getTopology(PlanProjectType)}, containing additional
     * information about the cached topology which is useful for topology pipeline sumaries.
     */
    public static class CachedTopologyResult {

        private final Map<Long, TopologyEntityDTO.Builder> entities;

        CachedTopologyResult(@Nonnull final Map<Long, TopologyEntityDTO.Builder> entities) {
            this.entities = entities;
        }

        @Nonnull
        public Map<Long, TopologyEntityDTO.Builder> getEntities() {
            return entities;
        }

        @Override
        public String toString() {
            StringBuilder summary = new StringBuilder();
            summary.append("Using cached topology of size ").append(entities.size());
            return summary.toString();
        }
    }

    /**
     * Keeps cached entities as {@link TopologyEntityDTO.Builder} clones.
     */
    private static class BuilderCachedEntities extends CachedEntities<TopologyEntityDTO.Builder> {
        @Override
        @Nullable
        Builder snapshotBuilder(Builder existing) {
            return existing.clone();
        }

        @Nullable
        @Override
        TopologyEntityDTO snapshotEntity(Builder existing) {
            return existing.build();
        }

        @Override
        Builder fromBuilder(Builder bldr) {
            return bldr.clone();
        }
    }

    /**
     * Keeps cached entities as serialized byte arrays to save memory (at the expense of
     * time to serialize).
     */
    private static class SerializedCachedEntities extends CachedEntities<byte[]> {

        @Override
        @Nullable
        Builder snapshotBuilder(byte[] existing) {
            try {
                TopologyEntityDTO.Builder b = TopologyEntityDTO.newBuilder();
                b.mergeFrom(existing);
                return b;
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        }

        @Nullable
        @Override
        TopologyEntityDTO snapshotEntity(byte[] existing) {
            try {
                return TopologyEntityDTO.parseFrom(existing);
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        }

        @Override
        byte[] fromBuilder(Builder bldr) {
            return bldr.build().toByteArray();
        }
    }

    /**
     * Flexible implementation to clone, cache, and retrieve {@link TopologyEntityDTO.Builder}s.
     *
     * @param <T> The type of object that the input {@link TopologyEntityDTO.Builder} is cloned into.
     */
    private abstract static class CachedEntities<T> {
        private Long2ObjectMap<T> cachedMap = Long2ObjectMaps.emptyMap();

        @Nullable
        abstract TopologyEntityDTO.Builder snapshotBuilder(T existing);

        @Nullable
        abstract TopologyEntityDTO snapshotEntity(T existing);

        abstract T fromBuilder(TopologyEntityDTO.Builder bldr);

        public boolean isEmpty() {
            return cachedMap.isEmpty();
        }

        public Stream<Builder> getAllEntityBuilders() {
            return cachedMap.values().stream()
                    .map(this::snapshotBuilder)
                    .filter(Objects::nonNull);
        }

        public Stream<TopologyEntityDTO> getEntities(List<Long> ids) {
            return ids.stream()
                    .map(cachedMap::get)
                    .filter(Objects::nonNull)
                    .map(this::snapshotEntity)
                    .filter(Objects::nonNull);
        }

        public void repopulate(Map<Long, Builder> newMap) {
            Long2ObjectMap<T> newCachedMap = new Long2ObjectOpenHashMap<>(newMap.size());
            newMap.values().forEach(t -> {
                newCachedMap.put(t.getOid(), fromBuilder(t));
            });
            cachedMap = newCachedMap;
        }
    }
}
