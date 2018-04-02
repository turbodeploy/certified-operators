package com.vmturbo.stitching.journal;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.zjsonpatch.DiffFlags;
import com.flipkart.zjsonpatch.JsonDiff;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonDiffField.RootJsonNode;

/**
 * A class for use in generating human-readable differences between two JSON object strings.
 *
 * The differ understands JSON semantics and is able to produce more semantically-relevant and
 * readable diffs than a basic diffing tool.
 */
public class JsonSemanticDiffer {
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSemanticDiffer() {
    }

    public DiffOutput semanticDiff(@Nonnull final String jsonObjectA,
                               @Nonnull final String jsonObjectB) throws IOException {
        return semanticDiff(jsonObjectA, jsonObjectB, Verbosity.LOCAL_CONTEXT_VERBOSITY,
            FormatRecommendation.PRETTY, 0);
    }

    public DiffOutput semanticDiff(@Nonnull final String jsonObjectA,
                                   @Nonnull final String jsonObjectB,
                                   @Nonnull final Verbosity verbosity,
                                   @Nonnull final FormatRecommendation format,
                                   final int spaceOffset) throws IOException {
        // Compute the differences
        final JsonNode nodeA = mapper.readTree(jsonObjectA);
        final JsonNode nodeB = mapper.readTree(jsonObjectB);
        final EnumSet<DiffFlags> flags = DiffFlags.dontNormalizeOpIntoMoveAndCopy();
        final JsonNode differences = JsonDiff.asJson(nodeA, nodeB, flags);

        final List<JsonDiffDescription> diffDescriptions =
            Arrays.asList(mapper.treeToValue(differences, JsonDiffDescription[].class));
        if (diffDescriptions.size() != differences.size()) {
            throw new IllegalStateException("Failed to parse JSON Diff correctly. Expected list of size " +
                differences.size() + " but got list of size " + diffDescriptions.size() + " for differences: " +
                differences);
        }
        for (int i = 0; i < diffDescriptions.size(); i++) {
            diffDescriptions.get(i).setAssociatedJsonDiff(differences.get(i));
        }

        // Wrap the original tree for a in JsonDiffNode objects to track the differences in a way
        // that associates the differences with the nodes.
        final JsonDiffField rootNode = JsonDiffField.newField(nodeA);
        rootNode.addChildren(nodeA);

        // Factor the differences into the tree
        final RootJsonNode rootJsonNode = new RootJsonNode(nodeA);
        diffDescriptions.forEach(diffDescription ->
            rootNode.factorDifference(diffDescription.createPathDeque(), rootJsonNode, diffDescription));

        // Render the diff to a StringBuilder.
        final StringBuilder builder = new StringBuilder(256);
        rootNode.renderToBuilder(builder, nodeA,
            SpaceRenderer.newRenderer(spaceOffset, 0, format), verbosity);

        return new DiffOutput(!rootNode.hasChanges(), builder.toString());
    }

    /**
     * The output of a call to generate a semantic diff.
     */
    public static class DiffOutput {
        /**
         * True if the diff was produced by diffing two identical JSON objects.
         */
        public final boolean identical;

        /**
         * The text contents of the diff.
         */
        public final String diff;

        public DiffOutput(final boolean identical,
                          @Nonnull final String diff) {
            this.identical = identical;
            this.diff = Objects.requireNonNull(diff);
        }
    }
}
