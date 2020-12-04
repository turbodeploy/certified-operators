package com.vmturbo.topology.processor.identity;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * The EntityDescriptor implements the entity descriptor.
 *
 * <p>The EntityDTOs will have to contain the information specific to a particular entity subtype.
 */
public interface EntityDescriptor {
    /**
     * Returns the identifying property list.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The identifying property list.
     */
    @Nonnull
    List<PropertyDescriptor> getIdentifyingProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor);

    /**
     * Returns the "volatile identifying" property set.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The "volatile identifying" property set.
     */
    @Nonnull
    List<PropertyDescriptor> getVolatileProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor);

    /**
     * Returns the "non-volatile identifying" property set.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The "non-volatile  identifying" property set.
     */
    @Nonnull
    List<PropertyDescriptor> getNonVolatileProperties(
        @Nonnull EntityMetadataDescriptor metadataDescriptor);

    /**
     * Returns the "heuristic" property set.
     *
     * @param metadataDescriptor The entity metadata descriptor.
     * @return The "heuristic" property set.
     */
    @Nonnull
    List<PropertyDescriptor> getHeuristicProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor);

    /**
     * Returns the Heuristics descriptor.
     *
     * @return The Heuristics descriptor.
     */
    @Nonnull
    HeuristicsDescriptor getHeuristicsDescriptor();
}
