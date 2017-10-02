package com.vmturbo.topology.processor.identity.services;

import java.util.Map;
import java.util.SortedMap;

/**
 * The VMTPolicyMatcher implements policy matcher.
 */
interface PolicyMatcher {
    /**
     * Checks whether two sets of properties match.
     * The sets are ordered by weight, thus being grouped by sub-class of properties.
     *
     * @param heuristicsOld The old heuristics property set.
     * @param heuristicsNew The new heuristics property set.
     * @return {@code true} in case of a match.
     */
    boolean matches(SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsOld,
                    SortedMap<Integer, Map<String, PropertyReferenceCounter>> heuristicsNew);
}
