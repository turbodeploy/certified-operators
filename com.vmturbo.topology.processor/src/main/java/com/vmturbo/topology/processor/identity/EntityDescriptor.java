package com.vmturbo.topology.processor.identity;

import java.util.Collection;

/**
 * The EntityDescriptor implements the entity descriptor.
 *
 * <p>The EntityDTOs will have to contain the information specific to a particular entity subtype.
 */
public interface EntityDescriptor {
    /**
     * Returns the "non-volatile identifying" property set.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The "non-volatile identifying" property set.
     * @exception IdentityWrongSetException In case the required properties aren't available.
     */
    Collection<PropertyDescriptor> getIdentifyingProperties(EntityMetadataDescriptor metadataDescriptor)
                    throws IdentityWrongSetException;

    /**
     * Returns the "volatile identifying" property set.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The "volatile identifying" property set.
     * @exception IdentityWrongSetException In case the required properties aren't available.
     */
    Collection<PropertyDescriptor> getVolatileProperties(EntityMetadataDescriptor metadataDescriptor)
                    throws IdentityWrongSetException;

    /**
     * Returns the "heuristic" property set.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The "heuristic" property set.
     * @exception IdentityWrongSetException In case the required properties aren't available.
     */
    Collection<PropertyDescriptor> getHeuristicProperties(EntityMetadataDescriptor metadataDescriptor)
                    throws IdentityWrongSetException;

    /**
     * Returns the Heuristics descriptor.
     *
     * @return The Heuristics descriptor.
     */
    HeuristicsDescriptor getHeuristicsDescriptor();
}
