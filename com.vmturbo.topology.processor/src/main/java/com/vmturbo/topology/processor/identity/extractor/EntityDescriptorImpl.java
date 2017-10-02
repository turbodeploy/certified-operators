package com.vmturbo.topology.processor.identity.extractor;

import java.util.Collection;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.HeuristicsDescriptor;
import com.vmturbo.topology.processor.identity.IdentityWrongSetException;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * Default entity descriptor.
 */
public class EntityDescriptorImpl implements EntityDescriptor {
    public final Collection<PropertyDescriptor> identifyingProperties;
    public final Collection<PropertyDescriptor> volatileProperties;
    public final Collection<PropertyDescriptor> heuristicProperties;

    public EntityDescriptorImpl(Collection<PropertyDescriptor> identifyingProperties,
                                Collection<PropertyDescriptor> volatileProperties,
                                Collection<PropertyDescriptor> heuristicProperties) {
        this.identifyingProperties = ImmutableList.copyOf(identifyingProperties);
        this.volatileProperties = ImmutableList.copyOf(volatileProperties);
        this.heuristicProperties = ImmutableList.copyOf(heuristicProperties);
    }

    @Override
    public Collection<PropertyDescriptor> getIdentifyingProperties(
            EntityMetadataDescriptor metadataDescriptor) throws IdentityWrongSetException {
        return identifyingProperties;
    }

    @Override
    public Collection<PropertyDescriptor> getVolatileProperties(
            EntityMetadataDescriptor metadataDescriptor) throws IdentityWrongSetException {
        return volatileProperties;
    }

    @Override
    public Collection<PropertyDescriptor> getHeuristicProperties(
            EntityMetadataDescriptor metadataDescriptor) throws IdentityWrongSetException {
        return heuristicProperties;
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
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("Non-volatile properties", identifyingProperties)
                          .add("Volatile properties", volatileProperties)
                          .add("Heuristic properties", heuristicProperties)
                          .toString();
    }
}
