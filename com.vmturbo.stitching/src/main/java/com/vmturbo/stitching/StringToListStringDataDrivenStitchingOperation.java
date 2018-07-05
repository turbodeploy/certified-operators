package com.vmturbo.stitching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

public abstract class StringToListStringDataDrivenStitchingOperation extends
        DataDrivenStitchingOperation<String, List<String>> {

    public StringToListStringDataDrivenStitchingOperation(
            @Nonnull StitchingMatchingMetaData<String, List<String>> matchingMetaData) {
        super(matchingMetaData);
    }

    /**
     * Return a concatenated list of the String values passed in.
     *
     * @param matchingValues List of single matching values returned by a matching field or property.
     * @return Optional string of concatenated values or Optional.empty.
     */
    @Override
    protected Optional<String> combineInternalSignatures(
            List<String> matchingValues) {
        return combineStringSignatures(matchingValues);
    }

    @Nonnull
    @Override
    public StitchingIndex<String, List<String>> createIndex(final int expectedSize) {
        return new ReverseListMembershipStitchingIndex(expectedSize);
    }

    /**
     * An index that permitting match identification based on membership in a list.  The
     * internal entity provides a property that returns a String.  The external
     * entity provides a property that is a list of Strings.  If that single String appears in
     * the list of Strings, we have a match.  Otherwise, we don't.
     *
     * This index maintains a map of each string that appears in the internal entity's list to
     * the entire list.
     */
    public static class ReverseListMembershipStitchingIndex implements
            StitchingIndex<String, List<String>> {

        private final Map<String, String> index;

        public ReverseListMembershipStitchingIndex(final int expectedSize) {
            index = new HashMap<>(expectedSize);
        }

        @Override
        public void add(@Nonnull String internalSignature) {
            index.put(internalSignature, internalSignature);
        }

        @Override
        public Stream<String> findMatches(@Nonnull List<String> externalSignature) {
            return externalSignature.stream()
                    .map(partnerMatchingValue -> index.get(partnerMatchingValue))
                    .filter(Objects::nonNull);
        }
    }
}

