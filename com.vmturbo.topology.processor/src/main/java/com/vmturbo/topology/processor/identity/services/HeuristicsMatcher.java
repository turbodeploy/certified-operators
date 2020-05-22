package com.vmturbo.topology.processor.identity.services;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.EntityMetadataDescriptor;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;

/**
 * The HeuristicsMatcher matches heuristic properties for identity service.
 */
public class HeuristicsMatcher {

    /**
     * Converts the list of heuristics to the ordered map by subclass weight.
     *
     * @param heuristics The heuristics.
     * @return The ordered map by subclass weight.
     */
    private SortedMap<Integer, Map<String, PropertyReferenceCounter>>
                    convertToOrderedComparables(Iterable<PropertyDescriptor> heuristics) {
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> map = new TreeMap<>();
        for (PropertyDescriptor descriptor : heuristics) {
            Map<String, PropertyReferenceCounter> values = map.get(descriptor.getPropertyTypeRank());
            if (values == null) {
                values = new HashMap<>();
                map.put(descriptor.getPropertyTypeRank(), values);
            }

            // Reference count for repeated elements
            String value = descriptor.getValue();
            PropertyReferenceCounter count = values.get(value);
            if (count == null) {
                count = new PropertyReferenceCounter();
            }
            count.increment();
            values.put(value, count);
        }

        return map;
    }

    /**
     * Locates the match. We call this when we must perform the heuristics match.
     *
     * @param heuristicsLast The heuristics from the last run.
     * @param heuristicsNow The heuristics from the previous request.
     * @param descriptor The entity in question.
     * @param metadata The metadata for the entity.
     * @return The {@code true} if matched.
     */
    public boolean locateMatch(@Nullable Iterable<PropertyDescriptor> heuristicsLast,
                    @Nonnull Iterable<PropertyDescriptor> heuristicsNow,
                    @Nonnull EntityDescriptor descriptor,
                    @Nonnull EntityMetadataDescriptor metadata) {
        // We do not have a previous incarnation, return null.
        if (heuristicsLast == null) {
            return false;
        }

        // We have previous and now.
        // Sort them according to the class (and its weight).
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld =
                        convertToOrderedComparables(heuristicsLast);
        SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew =
                        convertToOrderedComparables(heuristicsNow);

        PolicyMatcher matcher = null;
        switch (descriptor.getHeuristicsDescriptor().getPolicy()) {
            case AMOUNT_MATCH_DEFAULT:
                matcher = new PolicyMatcherDefault(metadata.getHeuristicThreshold());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported policy");
        }

        if (matcher.matches(heuristicsOld, heuristicsNew)) {
            return true;
        }

        // No match, return null.
        return false;
    }

}
