package com.vmturbo.topology.processor.identity;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

/**
 * A container for data required to identify a single entry in the
 * {@link IdentityServiceUnderlyingStore}.
 */
@Immutable
public class EntryData {

    private final EntityDescriptor entityDescriptor;
    private final EntityMetadataDescriptor metadataDescriptor;

    // The ID of the probe of the target which discovered this entity.
    private final long probeId;

    private final EntityType entityType;

    public EntryData(@Nonnull final EntityDescriptor entityDescriptor,
                     @Nonnull final EntityMetadataDescriptor metadataDescriptor,
                     final long probeId,
                     @Nonnull final EntityType entityType) {
        this.entityDescriptor = Objects.requireNonNull(entityDescriptor);
        this.metadataDescriptor = Objects.requireNonNull(metadataDescriptor);
        this.probeId = probeId;
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Nonnull
    public EntityDescriptor getDescriptor() {
        return entityDescriptor;
    }

    @Nonnull
    public EntityMetadataDescriptor getMetadata() {
        return metadataDescriptor;
    }

    public long getProbeId() {
        return probeId;
    }

    @Nonnull
    public EntityType getEntityType() {
        return entityType;
    }
}