package com.vmturbo.stitching.journal;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonDiffField.DifferenceType;

/**
 * A helper class for rendering whitespace and addition/deletion characters.
 */
public interface SpaceRenderer {

    @Nonnull
    SpaceRenderer childRenderer(final DifferenceType differenceType);

    @Nonnull
    String render();

    @Nonnull
    String prettyNewLine();

    @Nonnull
    String mandatoryNewLine();

    @Nonnull
    String space();

    static SpaceRenderer newRenderer(final int initialBlankOffset, final int initialOffset,
                                     final FormatRecommendation format) {
        if (format == FormatRecommendation.PRETTY) {
            return new PrettySpaceRenderer(initialBlankOffset, initialOffset, DifferenceType.UNMODIFIED);
        } else {
            return new CompactSpaceRenderer(initialBlankOffset, initialOffset, DifferenceType.UNMODIFIED,
                CompactSpaceRenderer.LineStatus.NEW_LINE, false);
        }
    }

    /**
     * Renders spaces in pretty format.
     */
    @Immutable
    class PrettySpaceRenderer implements SpaceRenderer {
        private final int initialBlankOffset;
        private final int nSpaces;
        private final DifferenceType differenceType;

        private PrettySpaceRenderer(final int initialBlankOffset,
                              final int nSpaces,
                              final DifferenceType differenceType) {
            this.initialBlankOffset = initialBlankOffset;
            this.nSpaces = nSpaces;
            this.differenceType = differenceType;
        }

        @Override
        @Nonnull
        public SpaceRenderer childRenderer(final DifferenceType differenceType) {
            if (differenceType == DifferenceType.UNMODIFIED) {
                return new PrettySpaceRenderer(initialBlankOffset, nSpaces + 2, this.differenceType);
            } else {
                return new PrettySpaceRenderer(initialBlankOffset, nSpaces + 2, differenceType);
            }
        }

        @Override
        @Nonnull
        public String render() {
            String character = " ";
            if (differenceType == DifferenceType.ADDITION) {
                character = "+";
            } else if (differenceType == DifferenceType.REMOVAL) {
                character = "-";
            }

            final String finalCharacter = character;
            return IntStream.range(0, initialBlankOffset)
                .mapToObj(index -> " ")
                .collect(Collectors.joining())
                +
                IntStream.range(0, nSpaces)
                    .mapToObj(index -> finalCharacter)
                    .collect(Collectors.joining());
        }

        @Override
        @Nonnull
        public String prettyNewLine() {
            return "\n";
        }

        @Override
        @Nonnull
        public String mandatoryNewLine() {
            return "\n";
        }

        @Override
        @Nonnull
        public String space() {
            return " ";
        }
    }

    /**
     * Compact format for rendering spaces.
     *
     * Unlike the {@link com.vmturbo.stitching.journal.SpaceRenderer.PrettySpaceRenderer}, this class
     * is not immutable.
     */
    class CompactSpaceRenderer implements SpaceRenderer {
        private final int initialBlankOffset;
        private final int nSpaces;
        private final DifferenceType differenceType;
        private LineStatus lineStatus;
        private boolean ancestorAddedOrRemoved;

        // TODO: Consider a representation that allows both new lines and additions/removals
        // at the same time, but for now this is unneeded.
        enum LineStatus {
            /**
             * Indicates nothing has been rendered yet on a line for the space renderer.
             */
            NEW_LINE,

            /**
             * Indicates the space renderer has already rendered something on the current line.
             */
            CONTINUATION,

            /**
             * Indicates the space renderer should render the characters for a difference type.
             */
            NEW_ADDITION_OR_REMOVAL
        }

        private CompactSpaceRenderer(final int initialBlankOffset,
                                     final int nSpaces,
                                     final DifferenceType differenceType,
                                     final LineStatus lineStatus,
                                     final boolean ancestorAddedOrRemoved) {
            this.initialBlankOffset = initialBlankOffset;
            this.nSpaces = nSpaces;
            this.differenceType = differenceType;
            this.lineStatus = lineStatus;
            this.ancestorAddedOrRemoved = ancestorAddedOrRemoved;
        }

        @Override
        @Nonnull
        public SpaceRenderer childRenderer(final DifferenceType differenceType) {
            if (differenceType == DifferenceType.UNMODIFIED) {
                return new CompactSpaceRenderer(initialBlankOffset, nSpaces + 2, this.differenceType,
                    lineStatus, ancestorAddedOrRemoved);
            } else {
                return new CompactSpaceRenderer(initialBlankOffset, nSpaces + 2, differenceType,
                    computeNextLineStatus(differenceType), ancestorAddedOrRemoved);
            }
        }

        private LineStatus computeNextLineStatus(final DifferenceType differenceType) {
            if (differenceType == DifferenceType.UNMODIFIED) {
                if (this.lineStatus == LineStatus.NEW_ADDITION_OR_REMOVAL) {
                    return LineStatus.CONTINUATION;
                } else {
                    return this.lineStatus;
                }
            }

            if (differenceType == DifferenceType.ADDITION || differenceType == DifferenceType.REMOVAL) {
                return LineStatus.NEW_ADDITION_OR_REMOVAL;
            }

            return this.lineStatus;
        }

        @Override
        @Nonnull
        public String render() {
            if (lineStatus == LineStatus.CONTINUATION) {
                // Continuations are always rendered with no spaces..
                return "";
            }

            String character = " ";
            if (differenceType == DifferenceType.ADDITION) {
                character = "+";
            } else if (differenceType == DifferenceType.REMOVAL) {
                character = "-";
            }

            final String finalCharacter = character;
            String spaces = "";
            if (lineStatus == LineStatus.NEW_LINE) {
                spaces = IntStream.range(0, initialBlankOffset)
                    .mapToObj(index -> " ")
                    .collect(Collectors.joining());
            } else if (lineStatus == LineStatus.NEW_ADDITION_OR_REMOVAL && !ancestorAddedOrRemoved) {
                ancestorAddedOrRemoved = true;
                spaces = IntStream.range(0, 2) // Only ever add two characters in compact format.
                    .mapToObj(index -> finalCharacter)
                    .collect(Collectors.joining());
            }

            // Since we have rendered something, the next render will be a continuation.
            lineStatus = LineStatus.CONTINUATION;
            return spaces;
        }

        @Override
        @Nonnull
        public String prettyNewLine() {
            return ""; // Compact returns pretty new lines as empty.
        }

        @Override
        @Nonnull
        public String mandatoryNewLine() {
            // Mandatory new lines actually return a new line. Also update the line status
            // to match.
            lineStatus = LineStatus.NEW_LINE;
            return "\n";
        }

        @Override
        @Nonnull
        public String space() {
            return ""; // Compact returns spaces as empty.
        }
    }
}
