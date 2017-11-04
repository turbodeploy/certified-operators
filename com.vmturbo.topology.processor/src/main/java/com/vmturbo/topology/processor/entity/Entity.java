package com.vmturbo.topology.processor.entity;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.topology.processor.conversions.Converter;

/**
 * Represents an entity that exists in the topology. The information for the
 * entity may be spread out across multiple targets and/or probes.
 */
public class Entity {

    /**
     * An ID assigned to this entity via the Identity Service.
     */
    private final long id;

    /**
     * An entity may have pieces of information from different targets.
     */
    private final Map<Long, PerTargetInfo> perTargetInfo = new ConcurrentHashMap<>();

    public Entity(final long id) {
        this.id = id;
    }

    public void addTargetInfo(final long targetId, @Nonnull final EntityDTO value) {
        perTargetInfo.put(targetId, Objects.requireNonNull(new PerTargetInfo(value)));
    }

    public void setHostedBy(final long targetId, final long hostEntity) {
        perTargetInfo.get(targetId).setHost(hostEntity);
    }

    @Nonnull
    public Optional<PerTargetInfo> getTargetInfo(final long targetId) {
        return Optional.ofNullable(perTargetInfo.get(targetId));
    }

    @Nonnull
    public Collection<PerTargetInfo> allTargetInfo() {
        return perTargetInfo.values();
    }

    @Nonnull
    public Set<Entry<Long, PerTargetInfo>> getPerTargetInfo() {
        return Collections.unmodifiableSet(perTargetInfo.entrySet());
    }

    @Nonnull
    public Set<Long> getTargets() {
        return Collections.unmodifiableSet(perTargetInfo.keySet());
    }

    public void removeTargetInfo(final long targetId) {
        perTargetInfo.remove(targetId);
    }

    public int getNumTargets() {
        return perTargetInfo.size();
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Entity OID: " + id + "\n" + perTargetInfo.entrySet().stream()
            .map(entry -> "Target " + entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("\n"));
    }

    /**
     * Create the {@link TopologyEntityDTO} representing this entity across all targets that discovered it.
     *
     * @param entityStore The {@link EntityStore} all entities are stored in.
     * @return The {@link TopologyEntityDTO} representing this entity, or an empty optional if this entity
     *         doesn't exist in the current topology.
     */
    @Nonnull
    public Optional<TopologyEntityDTO.Builder> constructTopologyDTO(
            @Nonnull final EntityStore entityStore) {
        if (perTargetInfo.isEmpty()) {
            return Optional.empty();
        }
        // TODO (roman, Aug 2016): Combine information from multiple EntityDTOs
        // For now just pick the first target, and use that EntityDTO for conversion.
        final Entry<Long, PerTargetInfo> targetAndEntityDTO =
                perTargetInfo.entrySet().iterator().next();

        return entityStore.getTargetEntityIdMap(targetAndEntityDTO.getKey())
            .map(entityIdMap -> Converter.newTopologyEntityDTO(
                    targetAndEntityDTO.getValue().entityInfo,
                    id,
                    entityIdMap));
    }

    /**
     * Information about this entity as discovered by a specific target.
     */
    public static class PerTargetInfo {

        private final EntityDTO entityInfo;

        // The physical machine on which the entity resides.
        // Only for VIRTUAL MACHINE -> PM (TODO (roman, Aug 2016): Solve this generally).
        private long physicalHost;

        public PerTargetInfo(@Nonnull final EntityDTO entityInfo) {
            this.entityInfo = entityInfo;
        }

        @Nonnull
        public EntityDTO getEntityInfo() {
            return entityInfo;
        }

        public long getHost() {
            return physicalHost;
        }

        void setHost(final long hostEntity) {
            physicalHost = hostEntity;
        }

        @Override
        public String toString() {
            return entityInfo.toString();
        }
    }
}
