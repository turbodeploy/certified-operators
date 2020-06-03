/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.stitching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * An index that permits matching based on a property that is a list of strings.
 * The rule for identifying a match by a list of strings is as follows:
 *
 * <p>For two entities with a matching property that is a list of strings, treat those lists as
 * sets and intersect them. If the intersection is empty, it is not a match. If the intersection
 * is non-empty it is a match.</p>
 *
 * <p>This index maintains a map of each string on the list to the entire list.</p>
 */
public class IntersectionStitchingIndex
                implements StitchingIndex<String, String> {
    private final Multimap<String, String> index;

    /**
     * Creates {@link IntersectionStitchingIndex} instance.
     *
     * @param expectedSize expected size of the map.
     */
    public IntersectionStitchingIndex(int expectedSize) {
        index = Multimaps.newSetMultimap(new HashMap<>(expectedSize), HashSet::new);
    }

    @Override
    public void add(@Nonnull String internalSignature) {
        index.put(internalSignature, internalSignature);
    }

    @Override
    public Stream<String> findMatches(@Nonnull String externalSignature) {
        return index.get(externalSignature).stream();
    }
}
