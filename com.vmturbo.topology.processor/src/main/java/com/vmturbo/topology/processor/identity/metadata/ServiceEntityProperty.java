package com.vmturbo.topology.processor.identity.metadata;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Descriptor for the property of an entity and it's associated groupId.
 */
public class ServiceEntityProperty {
    public final String name;
    public final int groupId;

    public ServiceEntityProperty(@Nonnull String name, int groupId) {
        Objects.requireNonNull(name);
        this.name = name;
        this.groupId = groupId;
    }

    @Override
    public String toString() {
        return String.format("%s --> %d", name, groupId);
    }
}
