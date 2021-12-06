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

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Represents an entity that exists in the topology. The information for the
 * entity may be spread out across multiple targets and/or probes.
 */
public class Entity {

    private static final String CONVERSION_ERROR = "Could not get entity dto from entity with id: %s";

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

    private static final Logger logger = LogManager.getLogger();

    private final boolean useSerializedEntities;


    public Entity(final long id, final EntityType entityType, boolean useSerializedEntities) {
        this.id = id;
        this.entityType = entityType;
        this.useSerializedEntities = useSerializedEntities;
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
        PerTargetInfo info;
        if (useSerializedEntities) {
            info = new SerializedPerTargetInfo(entityDTO);
        } else {
            info = new DeserializedPerTargetInfo(entityDTO);
        }
        perTargetInfo.put(targetId, info);
    }

    public void setHostedBy(final long targetId, final long hostEntity) {
        perTargetInfo.get(targetId).setHost(hostEntity);
    }

    @Nonnull
    public Optional<PerTargetInfo> getTargetInfo(final long targetId) {
        return Optional.ofNullable(perTargetInfo.get(targetId));
    }

    public String getConversionError() {
        return String.format(CONVERSION_ERROR, this.id);
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
     * Class that contains entity dto coming from different targets.
     */
    public interface PerTargetInfo {

        /**
         * Get the entity dto associated with this target.
         * @return the dto
         * @throws InvalidProtocolBufferException if the protobuf can't be serialized
         */
        @Nonnull
        EntityDTO getEntityInfo() throws InvalidProtocolBufferException;

        /**
         * Get the associated host if of this entity.
         * @return the host id
         */
        long getHost();

        /**
         * Get the entity dto builder associated with this target.
         * @return the dto
         * @throws InvalidProtocolBufferException if the protobuf can't be serialized
         */
        EntityDTO.Builder getEntityInfoBuilder() throws InvalidProtocolBufferException;

        /**
         * Set the host for this entity.
         * @param hostEntity the oid of the host
         */
        void setHost(long hostEntity);
    }

    /**
     * Non serialized information about this entity as discovered by a specific target.
     */
    public static class DeserializedPerTargetInfo implements PerTargetInfo {

        private final EntityDTO entityInfo;

        // The physical machine on which the entity resides.
        // Only for VIRTUAL MACHINE -> PM (TODO (roman, Aug 2016): Solve this generally).
        private long physicalHost;

        /**
         * Get an instance of a {@link DeserializedPerTargetInfo}.
         * @param entityInfo the corresponding entity dto
         */
        public DeserializedPerTargetInfo(@Nonnull final EntityDTO entityInfo) {
            this.entityInfo = entityInfo;
        }

        @Override
        @Nonnull
        public EntityDTO getEntityInfo() throws InvalidProtocolBufferException  {
            return entityInfo;
        }

        @Override
        public long getHost() {
            return physicalHost;
        }

        @Override
        public void setHost(final long hostEntity) {
            physicalHost = hostEntity;
        }

        @Override
        public String toString() {
            return entityInfo.toString();
        }

        @Override
        public Builder getEntityInfoBuilder() {
            return entityInfo.toBuilder();
        }
    }

    /**
     * Serialized information about this entity as discovered by a specific target.
     */
    public static class SerializedPerTargetInfo implements PerTargetInfo  {

        private final byte[] serializedEntity;
        private static final String ERORR_MESSAGE = "Could not parse entity dto from serialized entity";
        // The physical machine on which the entity resides.
        // Only for VIRTUAL MACHINE -> PM (TODO (roman, Aug 2016): Solve this generally).
        private long physicalHost;

        /**
         * Get an instance of a {@link SerializedPerTargetInfo}.
         * @param entityInfo the corresponding entity dto
         */
        public SerializedPerTargetInfo(@Nonnull final EntityDTO entityInfo) {
            serializedEntity = entityInfo.toByteArray();
        }

        @Override
        @Nonnull
        public EntityDTO getEntityInfo() throws InvalidProtocolBufferException {
            return EntityDTO.parseFrom(serializedEntity);
        }

        @Override
        @Nonnull
        public EntityDTO.Builder getEntityInfoBuilder() throws InvalidProtocolBufferException {
            return EntityDTO.newBuilder().mergeFrom(serializedEntity);
        }

        @Override
        public long getHost() {
            return physicalHost;
        }

        @Override
        public void setHost(final long hostEntity) {
            physicalHost = hostEntity;
        }

        @Override
        public String toString() {
            try {
                return getEntityInfo().toString();
            } catch (InvalidProtocolBufferException e) {
                logger.error(ERORR_MESSAGE, e);
                return ERORR_MESSAGE;
            }
        }
    }
}
