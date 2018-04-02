package com.vmturbo.stitching.journal;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonSemanticDiffer.DiffOutput;

/**
 * A {@link SemanticDiffer} can, given two entities of the same type, generate a semantic diff
 * of those objects at several different verbosity levels.
 */
public interface SemanticDiffer<T extends JournalableEntity<T>> {
    /**
     * Get the verbosity associated with this {@link SemanticDiffer}.
     *
     * @return The verbosity of the differ.
     */
    Verbosity getVerbosity();

    /**
     * Set the verbosity associated with this {@link SemanticDiffer}.
     *
     * @return The verbosity of the differ.
     */
    void setVerbosity(@Nonnull final Verbosity verbosity);

    /**
     * Generate a String describing the semantic difference between entities a and b.
     * These differences essentially describe the changes required to turn entity a into entity b.
     *
     * @param a The first entity. The generated semantic differences describe how to turn a into b.
     * @param b The second entity. The generated semantic differences describe how to turn a into b.
     * @param format The requested format that we should use when generating the diff. Not all
     *               SemanticDiffers may support all formats. Requesting an unsupported FormatRecommendation
     *               will result in an {@link IllegalArgumentException}.
     * @return A string describing the changes required to turn entity a into entity b.
     */
    @Nonnull
    String semanticDiff(@Nonnull final T a, @Nonnull final T b,
                        @Nonnull final FormatRecommendation format) throws IllegalArgumentException;

    /**
     * Determine for a given diff, whether that diff should be recorded as part of the total diff.
     *
     * @param difference The difference to check.
     * @return True if it should be part of the total difference, false otherwise.
     */
    default boolean shouldRecord(@Nonnull final DiffOutput difference) {
        return !difference.identical || getVerbosity() == Verbosity.COMPLETE_VERBOSITY;
    }

    /**
     * Dump a description of the entity protobuf.
     *
     * @param entity The entity to dump.
     * @return A string description of the entity.
     */
    @Nonnull
    String dumpEntity(@Nonnull final T entity);

    /**
     * The {@link EmptySemanticDiffer} always returns an empty string for every semantic difference.
     *
     * @param <T> The entity type of the entity being entered into the stitching journal.
     */
    class EmptySemanticDiffer<T extends JournalableEntity<T>> implements SemanticDiffer<T> {

        private Verbosity verbosity;

        public EmptySemanticDiffer(@Nonnull final Verbosity verbosity) {
            this.verbosity = verbosity;
        }

        @Override
        public Verbosity getVerbosity() {
            return verbosity;
        }

        @Override
        public void setVerbosity(@Nonnull final Verbosity verbosity) {
            this.verbosity = Objects.requireNonNull(verbosity);
        }

        @Nonnull
        @Override
        public String semanticDiff(@Nonnull final T a, @Nonnull final T b,
                                   @Nonnull final FormatRecommendation format) {
            // Format is ignored. Always returns empty.
            return "";
        }

        @Nonnull
        @Override
        public String dumpEntity(@Nonnull T entity) {
            return "";
        }
    }
}
