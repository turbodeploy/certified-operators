package com.vmturbo.stitching;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * A stitching operation for matching an internal signature that is a {@link List<String>} with an
 * external signature that is a String.  A match occurs if the internal signature contains the
 * external signature.
 */
public class ListStringToStringDataDrivenStitchingOperation extends
        DataDrivenStitchingOperation<List<String>, String> {

    public ListStringToStringDataDrivenStitchingOperation(
            @Nonnull StitchingMatchingMetaData<List<String>, String> matchingMetaData) {
        super(matchingMetaData);
    }

    /**
     * Return a concatenated list of the String values passed in.
     *
     * @param matchingValues List of single matching values returned by a matching field or property.
     * @return Optional string of concatenated values or Optional.empty.
     */
    @Override
    protected Optional<String> combineExternalSignatures(
            List<String> matchingValues) {
        return combineStringSignatures(matchingValues);
    }


    @Nonnull
    @Override
    public StitchingIndex<List<String>, String> createIndex(final int expectedSize) {
        return new ListMembershipStitchingIndex(expectedSize);
    }

    /**
     * An index that permitting match identification based on membership in a list.  The
     * internal entity provides a property that returns a list of Strings.  The external
     * entity provides a property that is a single String.  If that single String appears in
     * the list of Strings, we have a match.  Otherwise, we don't.  For example, for fabric
     * stitching, the internal entity PM gives a list of potential UUID matches and the external
     * PM matches if its UUID appears in the list.
     *
     * This index maintains a map of each string that appears in the internal entity's list to
     * the entire list.
     */
    public static class ListMembershipStitchingIndex implements
            StitchingIndex<List<String>, String> {

        private final Multimap<String, List<String>> index;

        public ListMembershipStitchingIndex(final int expectedSize) {
            index = Multimaps.newListMultimap(new HashMap<>(expectedSize), ArrayList::new);
        }

        @Override
        public void add(@Nonnull List<String> internalSignature) {
            internalSignature.forEach(matchingValue -> index.put(matchingValue, internalSignature));
        }

        @Override
        public Stream<List<String>> findMatches(@Nonnull String externalSignature) {
            return index.get(externalSignature).stream();
        }
    }
}
