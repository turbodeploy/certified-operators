package com.vmturbo.topology.processor.identity.services;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.vmturbo.platform.common.builders.metadata.EntityIdentityMetadataBuilder;

/**
 * The VMTPolicyMatcherDefault implements default heuristics property matcher.
 * Checks whether two sets of heuristic properties match to within 75%.
 */
public class PolicyMatcherDefault implements PolicyMatcher {

    /**
     * The threshold for determining a heuristic match using this matcher.
     * The value is a percentage and must be between 0 and 100 inclusive.
     */
    private final int heuristicThreshold;

    /**
     *  When a new heuristic identifying property is added, the existing
     *  records which don't have this new property will be
     *  assigned a placeholder dummy value. This DUMMY_VALUE will match
     *  with any new value assigned for that property. After it is matched,
     *  the dummy value will be replaced by the real value.
     *
     */
    public static final String DUMMY_VALUE = "DUMMY_VALUE";

    /**
     * Create a new default policy matcher.
     *
     * @param heuristicThreshold The threshold for determining a heuristic match using this matcher.
     *                           Value must be between 0 and 100.
     */
    public PolicyMatcherDefault(int heuristicThreshold) {
        if (!EntityIdentityMetadataBuilder.isValidHeuristicThreshold(heuristicThreshold)) {
            throw new IllegalArgumentException("The heuristicThreshold (" + heuristicThreshold
                + ") must be a value between 0 and 100 inclusive.");
        }

        this.heuristicThreshold = heuristicThreshold;
    }

    @Override
    public boolean matches(
            SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld,
            SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew) {
        Set<Integer> keySetOld = heuristicsOld.keySet();
        Set<Integer> keySetNew = heuristicsNew.keySet();

        // If we don't have the exact match between sub-classes, fail.
        // The equals() won't work, since it only checks whether this keySet is a superset of (or identical to) the other key set.
        // If the other key set is a superset of this one, the equals() method of comparison will not work.
        if (!keySetOld.containsAll(keySetNew) || !keySetNew.containsAll(keySetOld)) {
            return false;
        }

        // Iterate through all the values and compose the ratio
        int matches = 0;
        int total = 0;

        // Loop through all the subclasses by their weight.
        // Matches are calculated based on old values being present in the new set.
        for (Map.Entry<Integer, Map<String, PropertyReferenceCounter>> old : heuristicsOld
                .entrySet()) {
            Map<String, PropertyReferenceCounter> oldValues = old.getValue();
            Map<String, PropertyReferenceCounter> newValues = heuristicsNew.get(old.getKey());

            for (Map.Entry<String, PropertyReferenceCounter> entry : oldValues.entrySet()) {
                total += entry.getValue().intValue();
                // Always match to new value if the old value is set to dummy_value.
                final int count;
                if (entry.getKey().equals(DUMMY_VALUE)) {
                    count = 1;
                } else {
                    PropertyReferenceCounter i = newValues.get(entry.getKey());
                    // None, skip
                    if (i == null) {
                        continue;
                    }
                    count = i.intValue();
                }

                matches += Math.min(entry.getValue().intValue(), count);
            }
        }

        // Matches if we are above the threshold
        double threshold = new Integer(heuristicThreshold).doubleValue() / 100.;
        return (matches / (double)total >= threshold);
    }
}