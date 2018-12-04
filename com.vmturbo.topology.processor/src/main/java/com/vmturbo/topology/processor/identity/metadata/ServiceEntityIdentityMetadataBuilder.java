package com.vmturbo.topology.processor.identity.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.vmturbo.platform.common.builders.metadata.EntityIdentityMetadataBuilder;
import com.vmturbo.platform.common.builders.metadata.EntityIdentityMetadataBuilder.Classifier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;

/**
 * A utility class for building ServiceEntityIdentityMetadata objects from EntityIdentityMetadata
 *
 * Assigns unique 1-based groupId/rank integers (see {@code VMTPropertyDescriptor#getPropertyTypeRank})
 * to each property in the metadata. These ranks will be order-invariant (that is, the order of items
 * in the nonVolatile/volatile/heuristic lists does not affect the rank they are assigned
 */
@NotThreadSafe
public class ServiceEntityIdentityMetadataBuilder {

    /**
     * "entityType" will always be added to NonVolatile property lists
     */
    public static final String ENTITY_TYPE_PROPERTY_NAME = EntityType.getDescriptor().getName();

    private final EntityIdentityMetadata metadata;
    /**
     * Used to track names of properties given to the builder. A property must be unique, which we ensure
     * by checking allPropertyNames to see if a property has already been added.
     */
    private final Set<String> allPropertyNames = new HashSet<>();

    private int nextGroupId;

    /**
     * Used to create {@code ServiceEntityIdentityMetadata} from {@code EntityIdentityMetadata}.
     * It is illegal to specify a property more than once.
     *
     * @param metadata The {@code EntityIdentityMetadata}
     */
    public ServiceEntityIdentityMetadataBuilder(@Nonnull EntityIdentityMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Build a {@code ServiceEntityIdentityMetadata} instance from the metadata this builder
     * was constructed with.
     *
     * Always add the {@link EntityType} to the metadata to be compared. SE's with different
     * EntityType values are never equal.
     *
     * @return {@code ServiceEntityIdentityMetadata}
     */

    public ServiceEntityIdentityMetadata build() {
        synchronized (allPropertyNames) {
            nextGroupId = 1;
            validateHeuristicThreshold();
            List<ServiceEntityProperty> nonVolatileProperties = collectProperties(
                metadata.getNonVolatilePropertiesList(), Classifier.NON_VOLATILE);
            // always compare based on the EntityType
            addEntityTypeProperty(nonVolatileProperties);
            List<ServiceEntityProperty> volatileProperties = collectProperties(
                metadata.getVolatilePropertiesList(), Classifier.VOLATILE);
            List<ServiceEntityProperty> heuristicProperties = collectProperties(
                metadata.getHeuristicPropertiesList(), Classifier.HEURISTIC);

            return new ServiceEntityIdentityMetadata(
                nonVolatileProperties,
                volatileProperties,
                heuristicProperties,
                metadata.getHeuristicThreshold()
            );
        }
    }

    /**
     * Add the "entityType" property to the nonVolatileProperties list if "entityType" has not
     * yet been seen as a Metadata property.
     *
     * @param nonVolatileProperties the list of nonVolatileProperties to be added to
     */
    private void addEntityTypeProperty(@Nonnull final List<ServiceEntityProperty>
                                           nonVolatileProperties) {
        if (!allPropertyNames.contains(ENTITY_TYPE_PROPERTY_NAME)) {
            nonVolatileProperties.add(nextEntityProperty(ENTITY_TYPE_PROPERTY_NAME));
        }
    }

    /**
     * Collect the {@code ServiceEntityProperties} for a given classifier.
     *
     * Assigns a groupId to each ServiceEntityProperty object created. Also checks that the
     * property name is unique.
     *
     * @param propertyMetadata list of metadata properties to collect into ServiceEntityProperties
     * @param classifier Classifier associated with the list of properties
     * @return The PropertyMetadata transformed into ServiceEntityProperty objects.
     */
    private List<ServiceEntityProperty> collectProperties(
            List<PropertyMetadata> propertyMetadata,
            Classifier classifier) {
        List<ServiceEntityProperty> properties = new ArrayList<>();

        // Sort the metadata to ensure it is iterated the same way regardless of the order that the properties
        // were sent over the wire to us.
        List<PropertyMetadata> sortedMetadata = sortedMetadata(propertyMetadata);
        for (PropertyMetadata property : sortedMetadata) {
            String name = property.getName();
            if (allPropertyNames.contains(name)) {
                throw new IllegalArgumentException(
                    "Duplicate metadata " + classifier + " property \"" + name +
                        "\" in EntityIdentityMetadata" + metadata
                );
            }
            allPropertyNames.add(name);
            properties.add(nextEntityProperty(name));
        }

        return properties;
    }

    /**
     * Construct a new ServiceEntityProperty based on a name.
     *
     * Uses the 'nextGroupId' variable to index this property, and increment it so that
     * each ServiceEntityProperty will have a unique priority index.
     *
     * @param name the property name
     * @return a ServiceEntityProperty built from the name and the
     */
    private ServiceEntityProperty nextEntityProperty(final String name) {
        return new ServiceEntityProperty(name, nextGroupId++);
    }

    private List<PropertyMetadata> sortedMetadata(List<PropertyMetadata> propertyMetadata) {
        List<PropertyMetadata> sorted = new ArrayList<>(propertyMetadata);

        Collections.sort(sorted, new Comparator<PropertyMetadata>() {
            @Override
            public int compare(PropertyMetadata a, PropertyMetadata b) {
                return a.getName().compareTo(b.getName());
            }
        });

        return sorted;
    }

    private void validateHeuristicThreshold() {
        if (!EntityIdentityMetadataBuilder.isValidHeuristicThreshold(metadata.getHeuristicThreshold())) {
            throw new IllegalArgumentException("The heuristicThreshold (" + metadata.getHeuristicThreshold()
                    + ") must be a value between 0 and 100 inclusive.");
        }
    }
}
