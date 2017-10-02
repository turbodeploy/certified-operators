package com.vmturbo.topology.processor.identity.services;

import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * The EntityProxyDescriptor acts as a proxy to an {@link EntityDescriptor}.
 */
public interface EntityProxyDescriptor {

    /**
     * Getter for the OID.
     *
     * @return OID of the entity.
     */
    long getOID();

    /**
     * Get the query properties.
     *
     * @return The set of heuristic properties for the entity.
     */
    Iterable<PropertyDescriptor> getHeuristicProperties();
}
