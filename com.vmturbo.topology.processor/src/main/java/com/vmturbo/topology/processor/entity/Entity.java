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

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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

    /**
     * Type of the entity.
     */
    private final EntityType entityType;

    public Entity(final long id, final EntityType entityType) {
        this.id = id;
        this.entityType = entityType;
    }

    public void addTargetInfo(final long targetId, @Nonnull final EntityDTO entityDTO) {
        Objects.requireNonNull(entityDTO, "EntityDTO shouldn't be null");
        if (!entityDTO.hasEntityType() || entityDTO.getEntityType() != this.entityType) {
            throw new IllegalArgumentException(String.format(
                "EntityType from entity %s discovered by target: %s doesn't match. " +
                    "Expected: %s. Found: %s.\n" +
                    "Existing per-target information: %s\n" +
                    "New entity information: %s",
                entityDTO.getId(), targetId, this.entityType, entityDTO.getEntityType(),
                perTargetInfo.entrySet().stream()
                    .map(entry -> entry.getKey() + ": " + entry.getValue())
                    .collect(Collectors.joining("\n")),
                entityDTO));
        }

        perTargetInfo.put(targetId, new PerTargetInfo(entityDTO));
    }

    public void setHostedBy(final long targetId, final long hostEntity) {
        perTargetInfo.get(targetId).setHost(hostEntity);
    }

    @Nonnull
    public Optional<PerTargetInfo> getTargetInfo(final long targetId) {
        return Optional.ofNullable(perTargetInfo.get(targetId));
    }

    /**
     * Get the entity info for this entity
     * Uses the provided targetId if possible, but if no match for that targetId is found in the
     * perTargetInfo map, it returns the first entry in that map instead.
     *
     * The idea is that we want the entity info for this entity, regardless of what target it was
     * discovered by.
     *
     * @param targetId the target that discovered the desired entity data
     * @return information about this entity as discovered by a specific target.
     */
    @Nonnull
    public Optional<PerTargetInfo> getEntityInfo(final long targetId) {
        return Optional.ofNullable(this.perTargetInfo.get(targetId))
                // found the perTargetInfo - return as an Optional
                .map(Optional::of)
                // not found - look through all targetInfo and return the first one, if any
                .orElse(allTargetInfo().stream().findFirst());
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

    public EntityType getEntityType() {
        return entityType;
    }

    @Override
    public String toString() {
        return "Entity OID: " + id + "\n" + perTargetInfo.entrySet().stream()
            .map(entry -> "Target " + entry.getKey() + ": " + entry.getValue())
            .collect(Collectors.joining("\n"));
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
