package com.vmturbo.stitching;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * A stitching operations for stitching on matching data that returns a {@link List} for both the
 * internal and external signatures.  A match occurs if the two lists overlap.
 */
public class StringsToStringsDataDrivenStitchingOperation
                extends DataDrivenStitchingOperation<String, String> {
    /**
     * Creates {@link StringsToStringsDataDrivenStitchingOperation} instance.
     *
     * @param matchingMetaData metadata that contains information about properties
     *                 that should be used for stitching and the way how to parse values of those
     *                 properties.
     * @param stitchingScope probe categories that should be affected by this
     *                 operation.
     */
    public StringsToStringsDataDrivenStitchingOperation(
                    @Nonnull StitchingMatchingMetaData<String, String> matchingMetaData,
                    @Nonnull final Set<ProbeCategory> stitchingScope) {
        super(matchingMetaData, stitchingScope);
    }
}
