package com.vmturbo.topology.processor.identity.storage;

import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.composeKeyFromProperties;
import static com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore.propertyAsString;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

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
     * The query properties.
     */
    private final Set<String> queryPropertySet;

    /**
     * The query properties.
     */
    private final Collection<PropertyDescriptor> heuristicProperties;

    EntityInMemoryProxyDescriptor(final long oid,
                             @Nonnull final EntityDescriptor entityDescriptor,
                             @Nonnull final EntityMetadataDescriptor metadataDescriptor)
            throws IdentityWrongSetException {
        this(oid,
            entityDescriptor.getIdentifyingProperties(metadataDescriptor),
            entityDescriptor.getHeuristicProperties(metadataDescriptor));
    }

    EntityInMemoryProxyDescriptor(final long oid,
            @Nonnull final Collection<PropertyDescriptor> identifyingProperties,
            @Nonnull final Collection<PropertyDescriptor> heuristicProperties) {
        this.oid = oid;
        this.queryPropertySet = composeQuerySet(identifyingProperties, heuristicProperties);
        this.heuristicProperties = heuristicProperties;
        this.key = composeKeyFromProperties(identifyingProperties);
    }

    /**
     * Composes the query set. Adds all the properties with their ranks. Since each property type
     * will have its unique rank, this will work.
     *
     * @param identifyingProperties The identifying properties.
     * @param heuristicProperties   The heuristic properties.
     * @return The query set.
     */
    private @Nonnull Set<String> composeQuerySet(
            Collection<PropertyDescriptor> identifyingProperties,
            Collection<PropertyDescriptor> heuristicProperties) {
        // The values must be stored in the same order they are added.
        Set<String> queryPropertySet = new LinkedHashSet<>();
        for (PropertyDescriptor pd : identifyingProperties) {
            String propertyString = propertyAsString(pd);
            queryPropertySet.add(propertyString);
        }
        for (PropertyDescriptor pd : heuristicProperties) {
            String propertyString = propertyAsString(pd);
            queryPropertySet.add(propertyString);
        }
        return queryPropertySet;
    }

    public long getOID() {
        return oid;
    }

    public String getKey() {
        return key;
    }

    public Set<String> getQueryPropertySet() {
        return queryPropertySet;
    }

    @Override
    public Collection<PropertyDescriptor> getHeuristicProperties() {
        return heuristicProperties;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EntityInMemoryProxyDescriptor)) {
            return false;
        }

        @Nonnull EntityInMemoryProxyDescriptor that = (EntityInMemoryProxyDescriptor)obj;
        return (oid == that.oid && key.equals(that.key) &&
                queryPropertySet.equals(that.queryPropertySet) &&
                heuristicProperties.equals(that.heuristicProperties));
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, key, queryPropertySet, heuristicProperties);
    }
}
