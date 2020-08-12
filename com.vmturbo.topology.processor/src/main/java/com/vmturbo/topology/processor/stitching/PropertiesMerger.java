package com.vmturbo.topology.processor.stitching;

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
     */
    public void merge(
            @Nonnull final EntityDTO.Builder from,
            @Nonnull final EntityDTO.Builder onto) {
        if (strategy == MergePropertiesStrategy.JOIN) {
            final Set<String> existingProperties = onto.getEntityPropertiesList()
                    .stream()
                    .map(PropertiesMerger::createKey)
                    .collect(Collectors.toSet());
            from.getEntityPropertiesList()
                    .stream()
                    // If property with this name already exists in the destination DTO then skip
                    .filter(property -> !existingProperties.contains(createKey(property)))
                    .forEach(property -> onto.addEntityProperties(property.toBuilder()));
        }
    }

    private static String createKey(@Nonnull final EntityProperty property) {
        return property.getNamespace() + "::" + property.getName();
    }
}
