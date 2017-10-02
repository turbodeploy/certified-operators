package com.vmturbo.topology.processor.identity;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;

/**
 * Mock entity descriptor that actually persists data.
 */
public class EntityDescriptorMock implements EntityDescriptor {
    private final Collection<PropertyDescriptor> identifyingPropertiers;

    private final Collection<PropertyDescriptor> volatilePropertiers;

    private final Collection<PropertyDescriptor> heuristicPropertiers;

    public EntityDescriptorMock(List<String> identifyingPropertiers,
                                List<String> heuristicPropertiers) {
        this.identifyingPropertiers = composePropertySet(identifyingPropertiers);
        this.volatilePropertiers = composePropertySet(Collections.singleton("hyperv_vm"));
        this.heuristicPropertiers = composePropertySet(heuristicPropertiers);
    }

    @Override
    public Collection<PropertyDescriptor> getVolatileProperties(EntityMetadataDescriptor metadataDescriptor)
            throws IdentityWrongSetException {
        return volatilePropertiers;
    }

    @Override
    public Collection<PropertyDescriptor>
    getIdentifyingProperties(EntityMetadataDescriptor metadataDescriptor)
            throws IdentityWrongSetException {
        return identifyingPropertiers;
    }

    @Override
    public HeuristicsDescriptor getHeuristicsDescriptor() {
        return new HeuristicsDescriptor() {

            @Override
            public POLICY getPolicy() {
                return POLICY.AMOUNT_MATCH_DEFAULT;
            }
        };
    }

    @Override
    public Collection<PropertyDescriptor>
    getHeuristicProperties(EntityMetadataDescriptor metadataDescriptor)
            throws IdentityWrongSetException {
        return heuristicPropertiers;
    }

    public static Collection<PropertyDescriptor> composePropertySet(Collection<String> properties) {
        final AtomicInteger idCounter = new AtomicInteger(0);
        return properties.stream()
                .map(prop -> new PropertyDescriptorImpl(prop, idCounter.incrementAndGet()))
                .collect(Collectors.toList());
    }
}
