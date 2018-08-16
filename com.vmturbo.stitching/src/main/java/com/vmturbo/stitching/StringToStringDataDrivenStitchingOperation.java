package com.vmturbo.stitching;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * A stitching operation for matching an internal signature that is a {@link String} with an
 * external signature that is a {@link String}.  A match occurs if the internal signature equals the
 * external signature.
 */
public class StringToStringDataDrivenStitchingOperation extends
        DataDrivenStitchingOperation<String, String> {

    public StringToStringDataDrivenStitchingOperation(
            @Nonnull StitchingMatchingMetaData<String, String> matchingMetaData,
            @Nonnull final Set<ProbeCategory> stitchingScope) {
        super(matchingMetaData, stitchingScope);
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
