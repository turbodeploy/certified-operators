package com.vmturbo.topology.processor.stitching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

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
     * @param ontoPropertyMap A map of property-to-index of the properties of the onto
     *         entity. Passed in as a parameter and updated so that we do not have to create
     *         potentially large maps from the onto entity's properties when performing repeated
     *         merges onto the same entity.
     */
    public void merge(
            @Nonnull final EntityDTO.Builder from,
            @Nonnull final EntityDTO.Builder onto,
            @Nonnull final Map<String, Integer> ontoPropertyMap) {
        // TODO: Right now, entity tags are passed as entity properties in the VC_TAGS
        //  namespace. This is an inappropriate use of entity properties as we otherwise
        //  treat each entity property as a key,value pair, but tags are actually really
        //  key,List of values. In XL, tags are treated separately from entity properties,
        //  but for stitching, XL is required to give entity properties in the VC_TAGS
        //  namespace special treatment, since the merge is more complicated than it would be
        //  for simple key,value semantics. We should deprecate the current usage in EntityDTO
        //  and create a dedicated field in the EntityDTO for tags. Then we should consider
        //  getting rid of the namespace for entity properties and document clearly that each
        //  key can have only one value associated with it in entity properties.
        //  ---
        //  Remove fromProperties variable once tags have been moved out of entity properties as
        //  a part of https://vmturbo.atlassian.net/browse/OM-81057
        final List<EntityProperty> fromProperties = new ArrayList<>();

        if (strategy != MergePropertiesStrategy.MERGE_NOTHING) {
            from.getEntityPropertiesList()
                .forEach(property -> {
                    if (SDKUtil.VC_TAGS_NAMESPACE.equals(property.getNamespace())) {
                        onto.addEntityProperties(property);
                    } else {
                        fromProperties.add(property);
                    }
                });
        }

        switch (strategy) {
            case MERGE_IF_NOT_PRESENT:
                fromProperties.forEach(property -> {
                    final String propertyKey = createKey(property);
                    if (!ontoPropertyMap.containsKey(propertyKey)) {
                        ontoPropertyMap.put(propertyKey, onto.getEntityPropertiesList()
                            .size());
                        onto.addEntityProperties(property);
                    }
                });
                break;
            case MERGE_AND_OVERWRITE:
                fromProperties.forEach(property -> {
                    final String propertyKey = createKey(property);
                    if (ontoPropertyMap.containsKey(propertyKey)) {
                        onto.setEntityProperties(ontoPropertyMap.get(propertyKey), property);
                    } else {
                        ontoPropertyMap.put(propertyKey, onto.getEntityPropertiesList()
                            .size());
                        onto.addEntityProperties(property);
                    }
                });
                break;
            case MERGE_NOTHING:
            default:
                // no-op
        }
    }

    /**
     * Construct a map of property-to-index for the {@code onto} entity builder.
     *
     * @param onto The builder for the {@code onto} stitching entity.
     * @return The map of unique properties to indexes from the entity.
     */
    public Map<String, Integer> ontoPropertyMap(@Nonnull final EntityDTO.Builder onto) {
        return strategy == MergePropertiesStrategy.MERGE_NOTHING ? Collections.emptyMap()
                : IntStream.range(0, onto.getEntityPropertiesList().size()).boxed()
                        .filter(j -> !SDKUtil.VC_TAGS_NAMESPACE.equals(onto.getEntityPropertiesList().get(j).getNamespace()))
                        .collect(Collectors.toMap(i -> createKey(onto.getEntityPropertiesList().get(i)), Function.identity()));
    }

    private static String createKey(@Nonnull final EntityProperty property) {
        return property.getNamespace() + "::" + property.getName();
    }
}
