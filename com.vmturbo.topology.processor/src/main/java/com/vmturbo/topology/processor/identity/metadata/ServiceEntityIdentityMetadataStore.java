package com.vmturbo.topology.processor.identity.metadata;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;

/**
 * Stores metadata describing the identifying and heuristic properties of a service entity.
 */
public class ServiceEntityIdentityMetadataStore {
    private final Map<EntityType, ServiceEntityIdentityMetadata> metadataMap;

    /**
     * Build {@code ServiceEntityIdentityMetadata} from a list of {@code EntityIdentityMetadata}.
     * {@code EntityIdentityMetadata} will be automatically assigned groupIds to be used
     * by the identity service.
     * It is illegal to specify the {@code EntityIdentityMetadata} more than once.
     *
     * @param metadata List of metadata specifying identifying and heuristic properties.
     */
    public ServiceEntityIdentityMetadataStore(List<EntityIdentityMetadata> metadata) {
        metadataMap = new EnumMap<>(EntityType.class);

        for (EntityIdentityMetadata identityMetadata : metadata) {
            EntityType entityType = identityMetadata.getEntityType();

            if (metadataMap.containsKey(entityType)) {
                throw new IllegalArgumentException(
                    "Cannot add metadata for entity type " + entityType +
                        " because metadata was already defined for this entity type. " +
                        "Previous value was:\n" + metadataMap.get(entityType) +
                        "\n and new value is:\n" + format(identityMetadata)
                );
            } else {
                ServiceEntityIdentityMetadataBuilder metadataBuilder =
                    new ServiceEntityIdentityMetadataBuilder(identityMetadata);
                metadataMap.put(entityType, metadataBuilder.build());
            }
        }
    }

    /**
     * Get the set of all entity types that this store has information about.
     *
     * @return the set of all entity types that this store has information about
     */
    public Set<EntityType> supportedEntityTypes() {
        return metadataMap.keySet();
    }

    /**
     * Returns true if the store is empty (contains no metadata entries). False if it contains entries.
     *
     * @return True if the store is empty (contains no metadata entries). False if it contains entries.
     */
    public boolean isEmpty() {
       return metadataMap.isEmpty();
    }

    /**
     * Get the metadata for a given entity type. If the store does not contain any
     * metadata for that entity type, it will return null.
     *
     * @param entityType The entityType to get the metadata for.
     * @return ServiceEntityIdentityMetadata for the given entityType
     */
    public ServiceEntityIdentityMetadata getMetadata(EntityType entityType) {
        return metadataMap.get(entityType);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String newLine = System.getProperty("line.separator");

        for (Map.Entry<EntityType, ServiceEntityIdentityMetadata> entry : metadataMap.entrySet()) {
            builder.append("---------------------").append(newLine);
            builder.append(entry.getKey()).append(newLine);
            builder.append("---------------------").append(newLine);
            builder.append(entry.getValue().toString());
        }

        return builder.toString();
    }

    /**
     * Format an EntityIdentityMetadata into a string.
     * @param metadata the metadata to format to a string
     *
     * @return A string representation of the metadata.
     */
    private String format(EntityIdentityMetadata metadata) {
        String newLine = System.getProperty("line.separator");

        StringBuilder builder = new StringBuilder();
        builder.append("==NON-VOLATILE PROPERTIES==").append(newLine);
        for (PropertyMetadata property : metadata.getNonVolatilePropertiesList()) {
            builder.append("\t").append(property.getName()).append(newLine);
        }
        builder.append("==VOLATILE PROPERTIES==").append(newLine);
        for (PropertyMetadata property :  metadata.getVolatilePropertiesList()) {
            builder.append("\t").append(property.getName()).append(newLine);
        }
        builder.append("==HEURISTIC PROPERTIES==").append(newLine);
        for (PropertyMetadata property :  metadata.getHeuristicPropertiesList()) {
            builder.append("\t").append(property.getName()).append(newLine);
        }

        return builder.toString();
    }
}
