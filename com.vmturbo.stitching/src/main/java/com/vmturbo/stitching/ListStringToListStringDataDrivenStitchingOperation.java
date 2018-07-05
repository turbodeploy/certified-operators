package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * A stitching operations for stitching on matching data that returns a {@link List<String>} for
 * both the internal and external signatures.  A match occurs if the two lists overlap.
 */
public class ListStringToListStringDataDrivenStitchingOperation extends
        DataDrivenStitchingOperation<List<String>, List<String>> {

    public ListStringToListStringDataDrivenStitchingOperation(
            @Nonnull StitchingMatchingMetaData<List<String>, List<String>> matchingMetaData) {
        super(matchingMetaData);
    }

    @Nonnull
    @Override
    public StitchingIndex<List<String>, List<String>> createIndex(final int expectedSize) {
        return new ListIntersectionStitchingIndex(expectedSize);
    }

    /**
     * An index that permits matching based on a property that is a list of strings.
     * The rule for identifying a match by a list of strings is as follows:
     *
     * For two entities with a matching property that is a list of strings, treat those lists as
     * sets and intersect them. If the intersection is empty, it is not a match. If the intersection
     * is non-empty it is a match.
     *
     * This index maintains a map of each string on the list to the entire list.
     */
    public static class ListIntersectionStitchingIndex implements StitchingIndex<List<String>, List<String>> {

        private final Multimap<String, List<String>> index;

        public ListIntersectionStitchingIndex(final int expectedSize) {
            index = Multimaps.newListMultimap(new HashMap<>(expectedSize), ArrayList::new);
        }

        @Override
        public void add(@Nonnull List<String> internalSignature) {
            internalSignature.forEach(matchingValue -> index.put(matchingValue, internalSignature));
        }

        @Override
        public Stream<List<String>> findMatches(@Nonnull List<String> externalSignature) {
            return externalSignature.stream()
                    .flatMap(partnerMatchingValue -> index.get(partnerMatchingValue).stream());
        }
    }
}
