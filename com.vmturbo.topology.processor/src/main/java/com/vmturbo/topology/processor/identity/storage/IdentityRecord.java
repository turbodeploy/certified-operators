package com.vmturbo.topology.processor.identity.storage;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 *  Helper class for storing the entityType and identity descriptors for an entity.
 */
public class IdentityRecord {

    private final EntityType entityType;

    private final EntityInMemoryProxyDescriptor descriptor;

    public IdentityRecord(@Nonnull EntityType entityType,
                          @Nonnull EntityInMemoryProxyDescriptor memoryDescriptor) {

        this.entityType = Objects.requireNonNull(entityType);
        this.descriptor = Objects.requireNonNull(memoryDescriptor);
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public EntityInMemoryProxyDescriptor getDescriptor() {
        return descriptor;
    }
}
