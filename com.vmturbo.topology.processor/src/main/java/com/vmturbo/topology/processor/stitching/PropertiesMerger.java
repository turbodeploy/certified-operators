package com.vmturbo.topology.processor.stitching;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.stitching.utilities.MergePropertiesStrategy;

/**
 * Entity property merger. This class merges entity properties from source entity DTO to
 * destination DTO.
 */
public class PropertiesMerger {
    private final MergePropertiesStrategy strategy;

    /**
     * Constructs new {@code PropertyMerger}.
     *
     * @param strategy Merging strategy.
     */
    public PropertiesMerger(@Nonnull final MergePropertiesStrategy strategy) {
        this.strategy = Objects.requireNonNull(strategy);
    }

    /**
     * Merge entity properties from one entity DTO to another.
     *
     * @param from Source entity DTO builder.
     * @param onto Destination entity DTO builder.
     * @param ontoPropertySet The set of properties associated with the onto properties. Passed in as a parameter
     *                        and updated so that we do not have to recreate potentially large sets from the
     *                        onto entity's properties when performing repeated merges onto the same entity.
     */
    public void merge(
            @Nonnull final EntityDTO.Builder from,
            @Nonnull final EntityDTO.Builder onto,
            @Nonnull final Set<String> ontoPropertySet) {
        if (strategy == MergePropertiesStrategy.JOIN) {
            from.getEntityPropertiesList()
                    // If property with this name already exists in the destination DTO then skip
                    .forEach(property -> {
                        final String propertyKey = createKey(property);
                        if (!ontoPropertySet.contains(propertyKey)) {
                            ontoPropertySet.add(propertyKey);
                            onto.addEntityProperties(property);
                        }
                    });
        }
    }

    /**
     * Construct the set of properties for the {@code onto} entity builder.
     *
     * @param onto The builder for the {@code onto} stitching entity.
     * @return The set of unique properties from the entity.
     */
    public Set<String> ontoPropertySet(@Nonnull final EntityDTO.Builder onto) {
        return strategy == MergePropertiesStrategy.KEEP_ONTO
                ? Collections.emptySet()
                : onto.getEntityPropertiesList()
                        .stream()
                        .map(PropertiesMerger::createKey)
                        .collect(Collectors.toSet());
    }

    private static String createKey(@Nonnull final EntityProperty property) {
        return property.getNamespace() + "::" + property.getName();
    }
}
