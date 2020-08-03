package com.vmturbo.topology.processor.stitching;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Bundles together a {@link EntityDTO.Builder} together with the targetID that discovered the
 * corresponding entity as well as the OID for the entity.
 * <p>
 * A targetID-OID pair will uniquely distinguish an {@link EntityDTO} in the topology.
 * <p>
 * A {@link StitchingEntityData} is immutable in the sense that its targetId or the reference
 * to its {@link EntityDTO.Builder} cannot change, but the contained builder is mutable.
 */
public class StitchingEntityData {
    private final EntityDTO.Builder entityDtoBuilder;
    private final long targetId;
    private final long oid;
    private final long lastUpdatedTime;
    private final boolean supportsConnectedTo;

    /**
     * Create a new {@link StitchingEntityData} object for constructing a {@link TopologyStitchingEntity}.
     *
     * @param entityDtoBuilder The builder for the probe-discovered DTO.
     * @param targetId The ID of the target that discovered the DTO.
     * @param oid The OID (object ID) of the entity.
     * @param lastUpdatedTime The time at which the DTO was received from the probe.
     * @param supportsConnectedTo Indicates if this entity supports the connectedTo relationship.
     *                            If this is true layeredOver and consistsOf will be converted
     *                            to NORMAL and OWNS connectedTo relationships, respectively.
     */
    protected StitchingEntityData(@Nonnull final EntityDTO.Builder entityDtoBuilder,
            final long targetId,
            final long oid,
            final long lastUpdatedTime,
            final boolean supportsConnectedTo) {
        this.entityDtoBuilder = entityDtoBuilder;
        this.targetId = targetId;
        this.oid = oid;
        this.lastUpdatedTime = lastUpdatedTime;
        this.supportsConnectedTo = supportsConnectedTo;
    }

    public EntityDTO.Builder getEntityDtoBuilder() {
        return entityDtoBuilder;
    }

    public long getTargetId() {
        return targetId;
    }

    public long getOid() {
        return oid;
    }

    public String getLocalId() {
        return entityDtoBuilder.getId();
    }

    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public boolean supportsDeprecatedConnectedTo() {
        return supportsConnectedTo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityDtoBuilder, targetId);
    }

    /**
     * In the interest of performance, we do a REFERENCE EQUALS comparison
     * on the entityDtoBuilder member. StitchingEntityData will NOT be equal to another
     * StitchingEntityData if their builders contain the same contents but
     * are not the same object by reference. Because of their intended use,
     * the behavior should be the same if used properly but clients of this
     * class should be aware of the contract of the equals method here.
     *
     * @param other The other {@link StitchingEntityData to compare}.
     * @return If this is equal to the other, comparing the entityDtoBuilder
     *         by reference.
     */
    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof StitchingEntityData)) {
            return false;
        }

        final StitchingEntityData otherEntityData = (StitchingEntityData)other;
        return targetId == otherEntityData.targetId &&
                oid == otherEntityData.oid &&
                entityDtoBuilder == otherEntityData.entityDtoBuilder;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(Long.toString(getOid()))
            .add("localId", getLocalId())
            .add("targetId", targetId)
            .toString();
    }

    /**
     * A builder for creating a {@link StitchingEntityData} object.
     */
    public static class Builder {
        private final EntityDTO.Builder entityDtoBuilder;
        private long targetId;
        private long oid;
        private long lastUpdatedTime;
        private boolean supportsConnectedTo;

        private Builder(@Nonnull final EntityDTO.Builder builder) {
            this.entityDtoBuilder = Objects.requireNonNull(builder);
        }

        /**
         * Set the target Id.
         *
         * @param targetId The id of the target that discovered this entity.
         * @return A reference to {@link this} to support method chaining.
         */
        public Builder targetId(final long targetId) {
            this.targetId = targetId;
            return this;
        }

        /**
         * Set the oid.
         *
         * @param oid The Object Identifier (OID) for this entity.
         * @return A reference to {@link this} to support method chaining.
         */
        public Builder oid(final long oid) {
            this.oid = oid;
            return this;
        }

        /**
         * Set the last updated time.
         *
         * @param lastUpdatedTime The timestamp at which this entity was last updated.
         * @return A reference to {@link this} to support method chaining.
         */
        public Builder lastUpdatedTime(final long lastUpdatedTime) {
            this.lastUpdatedTime = lastUpdatedTime;
            return this;
        }

        /**
         * Indicates whether or not this DTO supoorts connectedTo relationships.  If true,
         * layeredOver and consistsOf in the DTO will be converted to NORMAL and OWNS connectedTo
         * relationships, respectively.
         *
         * @param supportsConnectedTo True if connectedTo is supported, false otherwise.
         * @return A reference to {@link this} to support method chaining.
         */
        public Builder supportsConnectedTo(final boolean supportsConnectedTo) {
            this.supportsConnectedTo = supportsConnectedTo;
            return this;
        }

        public StitchingEntityData build() {
            return new StitchingEntityData(entityDtoBuilder, targetId, oid, lastUpdatedTime,
                    supportsConnectedTo);
        }
    }

    public static Builder newBuilder(@Nonnull final EntityDTO.Builder entityDtoBuilder) {
        return new Builder(entityDtoBuilder);
    }
}
