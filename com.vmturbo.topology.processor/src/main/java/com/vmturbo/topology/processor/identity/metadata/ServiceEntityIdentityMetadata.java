package com.vmturbo.topology.processor.identity.metadata;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.collect.ImmutableList;

import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;

/**
 * Metadata describing the identifying and heuristic properties for a service entity.
 */
public class ServiceEntityIdentityMetadata implements EntityMetadataDescriptor {
    private final List<ServiceEntityProperty> nonVolatileProperties;
    private final List<ServiceEntityProperty> volatileProperties;
    private final List<ServiceEntityProperty> heuristicProperties;
    private final int heuristicThreshold;

    public ServiceEntityIdentityMetadata(@Nonnull List<ServiceEntityProperty> nonVolatileProperties,
                                         @Nonnull List<ServiceEntityProperty> volatileProperties,
                                         @Nonnull List<ServiceEntityProperty> heuristicProperties,
                                         int heuristicThreshold) {
        this.nonVolatileProperties = ImmutableList.copyOf(nonVolatileProperties);
        this.volatileProperties = ImmutableList.copyOf(volatileProperties);
        this.heuristicProperties = ImmutableList.copyOf(heuristicProperties);
        this.heuristicThreshold = heuristicThreshold;
    }

    public List<ServiceEntityProperty> getNonVolatileProperties() {
        return nonVolatileProperties;
    }

    public List<ServiceEntityProperty> getVolatileProperties() {
        return volatileProperties;
    }

    public List<ServiceEntityProperty> getHeuristicProperties() {
        return heuristicProperties;
    }

    @Override
    public String toString() {
        String newLine = System.getProperty("line.separator");

        StringBuilder builder = new StringBuilder();
        builder.append("NON-VOLATILE PROPERTIES (").append(nonVolatileProperties.size()).append(")").append(newLine);
        for (ServiceEntityProperty property : nonVolatileProperties) {
            builder.append("\t").append(property).append(newLine);
        }
        builder.append("VOLATILE PROPERTIES (").append(volatileProperties.size()).append(")").append(newLine);
        for (ServiceEntityProperty property : volatileProperties) {
            builder.append("\t").append(property).append(newLine);
        }
        builder.append("HEURISTIC PROPERTIES (").append(heuristicProperties.size()).append(")").append(newLine);
        for (ServiceEntityProperty property : heuristicProperties) {
            builder.append("\t").append(property).append(newLine);
        }

        return builder.toString();
    }

    @Override
    public int getMetadataVersion() {
        // Do we actually need this method yet?
        throw new NotImplementedException();
    }

    @Override
    public String getEntityType() {
        // Do we actually need this method yet?
        throw new NotImplementedException();
    }

    @Override
    public String getTargetType() {
        // Do we actually need this method yet?
        throw new NotImplementedException();
    }

    @Override
    public Collection<Integer> getIdentifyingPropertyRanks() {
        // Do we actually need this method yet?
        throw new NotImplementedException();
    }

    @Override
    public Collection<Integer> getVolatilePropertyRanks() {
        return volatileProperties.stream().map(prop -> prop.groupId).collect(Collectors.toList());
    }

    @Override
    public Collection<Integer> getNonVolatilePropertyRanks() {
        return nonVolatileProperties.stream().map(prop -> prop.groupId).collect(Collectors.toList());
    }

    @Override
    public Collection<Integer> getHeuristicPropertyRanks() {
        // Do we actually need this method yet?
        throw new NotImplementedException();
    }

    @Override
    public int getHeuristicThreshold() {
        return heuristicThreshold;
    }
}
