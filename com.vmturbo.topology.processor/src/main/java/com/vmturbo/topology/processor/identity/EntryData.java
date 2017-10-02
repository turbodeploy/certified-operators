package com.vmturbo.topology.processor.identity;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

/**
 * A container for data required to identify a single entry in the
 * {@link IdentityServiceUnderlyingStore}.
 */
@Immutable
public class EntryData {
    private final EntityDescriptor entityDescriptor;
    private final EntityMetadataDescriptor metadataDescriptor;

    /**
     * The {@link EntityDTO} associated with this entry, if any.
     * <p>
     * Note - this is for convenience, so that users of {@link IdentityServiceUnderlyingStore}
     * can keep track of which {@link EntityDTO}s are represented by which {@link EntryData}.
     */
    private final Optional<EntityDTO> entityDTO;

    public EntryData(@Nonnull final EntityDescriptor entityDescriptor,
                     @Nonnull final EntityMetadataDescriptor metadataDescriptor,
                     @Nullable final EntityDTO entityDTO) {
        this.entityDescriptor = entityDescriptor;
        this.metadataDescriptor = metadataDescriptor;
        this.entityDTO = Optional.ofNullable(entityDTO);
    }

    @Nonnull
    public EntityDescriptor getDescriptor() {
        return entityDescriptor;
    }

    @Nonnull
    public EntityMetadataDescriptor getMetadata() {
        return metadataDescriptor;
    }

    @Nonnull
    public Optional<EntityDTO> getEntityDTO() {
        return entityDTO;
    }
}
