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
 * A stitching operation for matching an internal signature that is a {@link String} with an
 * external signature that is a {@link String}.  A match occurs if the internal signature equals the
 * external signature.
 */
public class StringToStringDataDrivenStitchingOperation extends
        DataDrivenStitchingOperation<String, String> {

    public StringToStringDataDrivenStitchingOperation(
            @Nonnull StitchingMatchingMetaData<String, String> matchingMetaData) {
        super(matchingMetaData);
    }

    /**
     * Return a concatenated list of the String values passed in.
     *
     * @param matchingValues List of single matching values returned by a matching field or property.
     * @return Optional string of concatenated values or Optional.empty.
     */
    private Optional<String> combineSignatures(List<String> matchingValues) {
        return combineStringSignatures(matchingValues);
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
        return combineSignatures(matchingValues);
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
        return combineSignatures(matchingValues);
    }
}
