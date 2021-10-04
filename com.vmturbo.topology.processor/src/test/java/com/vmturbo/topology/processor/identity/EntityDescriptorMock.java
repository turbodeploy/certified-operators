package com.vmturbo.topology.processor.identity;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;

/**
 * Mock entity descriptor that actually persists data.
 */
public class EntityDescriptorMock implements EntityDescriptor {

    private final List<PropertyDescriptor> volatilePropertiers;

    private final List<PropertyDescriptor> heuristicPropertiers;

    private final List<PropertyDescriptor> nonVolatileProperties;

    /**
     * Constructs entity descriptor mock.
     *
     * @param nonVolatileProperties non-volatile properties
     * @param volatileProperties volatile properties
     * @param heuristicProperties euristic properties
     */
    public EntityDescriptorMock(List<String> nonVolatileProperties,
                                List<String> volatileProperties,
                                List<String> heuristicProperties) {
        this.nonVolatileProperties = composePropertySet(nonVolatileProperties, 1);
        this.volatilePropertiers = composePropertySet(volatileProperties, 2);
        this.heuristicPropertiers = composePropertySet(heuristicProperties, 3);
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getVolatileProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return volatilePropertiers;
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getNonVolatileProperties(
        @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return nonVolatileProperties;
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getIdentifyingProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return Stream.concat(nonVolatileProperties.stream(), volatilePropertiers.stream())
                .collect(Collectors.toList());
    }

    @Override
    @Nonnull
    public HeuristicsDescriptor getHeuristicsDescriptor() {
        return new HeuristicsDescriptor() {

            @Override
            public POLICY getPolicy() {
                return POLICY.AMOUNT_MATCH_DEFAULT;
            }
        };
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getHeuristicProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return heuristicPropertiers;
    }

    /**
     * Composes property set.
     *
     * @param properties properties
     * @param rank the rank of the properties
     * @return property descriptors
     */
    public static List<PropertyDescriptor> composePropertySet(@Nonnull List<String> properties, int rank) {
        return properties.stream()
                .map(prop -> new PropertyDescriptorImpl(prop, rank))
                .collect(Collectors.toList());
    }
}
