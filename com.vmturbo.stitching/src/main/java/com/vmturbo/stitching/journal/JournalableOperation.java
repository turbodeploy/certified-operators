package com.vmturbo.stitching.journal;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;

/**
 * An interface for a type of stitching operation whose changes to the topology can be traced
 * through the stitching journal.
 */
public interface JournalableOperation {
    /**
     * Get the recommended for the formatting to use in the stitching journal for changes
     * made by this operation.
     *
     * Specific operation implementations should override based on the nature of the changes
     * to the topology that they make.
     *
     * @return A context recommendation for level of detail to record in the stitching journal.
     */
    @Nonnull
    default FormatRecommendation getFormatRecommendation() {
        return FormatRecommendation.PRETTY;
    }

    /**
     * Get the name of the operation for use in displaying it in the {@link IStitchingJournal}.
     *
     * @return the name of the operation.
     */
    @Nonnull
    default String getOperationName() {
        return getClass().getSimpleName();
    }
}
