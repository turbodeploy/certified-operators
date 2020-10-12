package com.vmturbo.topology.processor.identity.storage;

import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.composeKeyFromProperties;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;

import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.IdentityWrongSetException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.services.EntityProxyDescriptor;

/**
 * The EntityInMemoryProxyDescriptor implements entity proxy descriptor. This is the
 * implementation tied to a particular implementation of the underlying store.
 */
@Immutable
public class EntityInMemoryProxyDescriptor implements EntityProxyDescriptor {

    /**
     * The OID.
     */
    private final long oid;

    /**
     * The key. We keep it so that we don't have to reconstruct it when we perform an update.
     */
    private final String key;

    /**
     * The identifying properties.
     * Includes volatile and non-volatile properties.
     */
    private final List<PropertyDescriptor> identifyingProperties;

    /**
     * The query properties.
     */
    private final List<PropertyDescriptor> heuristicProperties;

    public EntityInMemoryProxyDescriptor(final long oid,
                             @Nonnull final EntityDescriptor entityDescriptor,
                             @Nonnull final EntityMetadataDescriptor metadataDescriptor)
            throws IdentityWrongSetException {
        this(oid,
            entityDescriptor.getIdentifyingProperties(metadataDescriptor),
            entityDescriptor.getHeuristicProperties(metadataDescriptor));
    }

    public EntityInMemoryProxyDescriptor(final long oid,
            @Nonnull final List<PropertyDescriptor> identifyingProperties,
            @Nonnull final List<PropertyDescriptor> heuristicProperties) {
        this.oid = oid;
        this.identifyingProperties = identifyingProperties;
        this.heuristicProperties = ImmutableList.copyOf(heuristicProperties);
        this.key = composeKeyFromProperties(identifyingProperties);
    }

    public long getOID() {
        return oid;
    }

    public String getKey() {
        return key;
    }

    /**
     * Return whether or not this descriptor contains all the input property descriptors.
     *
     * @param properties The input property descriptors.
     * @return True if each of the input descriptors matches a descriptor for this entity.
     */
    public boolean containsAll(Iterable<PropertyDescriptor> properties) {
        // Note - the only time we use this we actually don't care about the heuristic properties
        // OR the volatile properties, because we are only looking up by non-volatile identifying properties.
        for (PropertyDescriptor pd : properties) {
            if (!(identifyingProperties.contains(pd) || heuristicProperties.contains(pd))) {
                return false;
            }
        }
        return true;
    }

    public Collection<PropertyDescriptor> getIdentifyingProperties() {
        return identifyingProperties;
    }

    @Override
    public Collection<PropertyDescriptor> getHeuristicProperties() {
        return heuristicProperties;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof EntityInMemoryProxyDescriptor)) {
            return false;
        }

        EntityInMemoryProxyDescriptor that = (EntityInMemoryProxyDescriptor)obj;
        return (oid == that.oid
            && key.equals(that.key)
            && heuristicProperties.equals(that.heuristicProperties));
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, key, identifyingProperties, heuristicProperties);
    }
}
