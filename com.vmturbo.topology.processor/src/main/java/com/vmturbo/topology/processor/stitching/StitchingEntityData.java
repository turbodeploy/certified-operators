package com.vmturbo.topology.processor.stitching;

import java.util.Objects;

import javax.annotation.Nonnull;

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

    public StitchingEntityData(@Nonnull final EntityDTO.Builder entityDtoBuilder,
                               final long targetId,
                               final long oid) {
        this.entityDtoBuilder = entityDtoBuilder;
        this.targetId = targetId;
        this.oid = oid;
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

    @Override
    public int hashCode() {
        return Objects.hash(entityDtoBuilder, targetId, targetId);
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

        final StitchingEntityData otherEntityData = (StitchingEntityData) other;
        return targetId == otherEntityData.targetId &&
            oid == otherEntityData.oid &&
            entityDtoBuilder == otherEntityData.entityDtoBuilder;
    }
}
