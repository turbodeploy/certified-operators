package com.vmturbo.topology.processor.identity;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;

/**
 * Mock entity descriptor that actually persists data.
 */
public class EntityDescriptorMock implements EntityDescriptor {
    private final List<PropertyDescriptor> identifyingPropertiers;

    private final List<PropertyDescriptor> volatilePropertiers;

    private final List<PropertyDescriptor> heuristicPropertiers;

    /**
     * Constructs entity descriptor mock.
     *
     * @param identifyingPropertiers identifying properties
     * @param heuristicPropertiers heuristic properties
     */
    public EntityDescriptorMock(List<String> identifyingPropertiers,
                                List<String> heuristicPropertiers) {
        this.identifyingPropertiers = composePropertySet(identifyingPropertiers);
        this.volatilePropertiers = composePropertySet(Collections.singletonList("hyperv_vm"));
        this.heuristicPropertiers = composePropertySet(heuristicPropertiers);
    }

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
        this.identifyingPropertiers = composePropertySet(nonVolatileProperties);
        this.volatilePropertiers = composePropertySet(volatileProperties);
        this.identifyingPropertiers.addAll(this.volatilePropertiers);
        this.heuristicPropertiers = composePropertySet(heuristicProperties);
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getVolatileProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return volatilePropertiers;
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getIdentifyingProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return identifyingPropertiers;
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
     * @return property descriptors
     */
    public static List<PropertyDescriptor> composePropertySet(@Nonnull List<String> properties) {
        final AtomicInteger idCounter = new AtomicInteger(0);
        return properties.stream()
                .map(prop -> new PropertyDescriptorImpl(prop, idCounter.incrementAndGet()))
                .collect(Collectors.toList());
    }
}
