package com.vmturbo.topology.processor.identity.extractor;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.HeuristicsDescriptor;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * Default entity descriptor.
 */
public class EntityDescriptorImpl implements EntityDescriptor {
    private final List<PropertyDescriptor> identifyingProperties;
    private final List<PropertyDescriptor> volatileProperties;
    private final List<PropertyDescriptor> nonVolatileProperties;
    private final List<PropertyDescriptor> heuristicProperties;

    /**
     * Constructs entity descriptor.
     *
     * @param nonVolatileProperties non-volatile properties
     * @param volatileProperties volatile properties
     * @param heuristicProperties heuristic properties
     */
    public EntityDescriptorImpl(@Nonnull List<PropertyDescriptor> nonVolatileProperties,
            @Nonnull List<PropertyDescriptor> volatileProperties,
            @Nonnull List<PropertyDescriptor> heuristicProperties) {
        this.nonVolatileProperties = ImmutableList.copyOf(nonVolatileProperties);
        this.volatileProperties = ImmutableList.copyOf(volatileProperties);
        this.heuristicProperties = ImmutableList.copyOf(heuristicProperties);
        this.identifyingProperties = new ArrayList<>(nonVolatileProperties);
        this.identifyingProperties.addAll(volatileProperties);
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getIdentifyingProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return identifyingProperties;
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getVolatileProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return volatileProperties;
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getNonVolatileProperties(
        @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return nonVolatileProperties;
    }

    @Override
    @Nonnull
    public List<PropertyDescriptor> getHeuristicProperties(
            @Nonnull EntityMetadataDescriptor metadataDescriptor) {
        return heuristicProperties;
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
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("Non-volatile properties", identifyingProperties)
                          .add("Volatile properties", volatileProperties)
                          .add("Heuristic properties", heuristicProperties)
                          .toString();
    }
}
