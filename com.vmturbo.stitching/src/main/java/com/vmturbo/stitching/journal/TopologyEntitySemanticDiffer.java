package com.vmturbo.stitching.journal;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonSemanticDiffer.DiffOutput;

/**
 * An object capable of generating semantic differences between {@link TopologyEntity}
 * instances.
 *
 * These diffs may be compact and can format either prettily or compactly.
 */
public class TopologyEntitySemanticDiffer implements SemanticDiffer<TopologyEntity> {

    private static final Logger logger = LogManager.getLogger();

    private Verbosity verbosity;
    private JsonSemanticDiffer differ;

    private final Gson gson = ComponentGsonFactory.createGson();
    private final Gson noPrettyGson = ComponentGsonFactory.createGsonNoPrettyPrint();

    public TopologyEntitySemanticDiffer(@Nonnull final Verbosity verbosity) {
        this.verbosity = verbosity;
        differ = new JsonSemanticDiffer();
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
    public String semanticDiff(@Nonnull final TopologyEntity a, @Nonnull final TopologyEntity b,
                               @Nonnull final FormatRecommendation format) {
        try {
            final StringBuilder builder = new StringBuilder();
            builder.append(entityDescription(a));

            if (format == FormatRecommendation.PRETTY) {
                builder.append("\n");
            }

            boolean changed = appendEntityDiff(a, b, builder, format);

            // If nothing changed, return empty.
            return changed ? StringUtils.strip(builder.toString()) : "";
        } catch (Exception e) {
            logger.error("Error generating semantic differences: ", e);
            return "";
        }
    }

    @Nonnull
    @Override
    public String dumpEntity(@Nonnull TopologyEntity entity) {
        return noPrettyGson.toJson(entity.getTopologyEntityImpl().toProto());
    }

    @Nonnull
    public JsonSemanticDiffer getJsonSemanticDiffer() {
        return differ;
    }

    @Nonnull
    public static String entityDescription(@Nonnull final TopologyEntity entity) {
        return String.format("%s oid-%d %s",
            entity.getJournalableEntityType().name(), entity.getOid(), entity.getDisplayName());
    }

    private boolean appendEntityDiff(@Nonnull final TopologyEntity entityA,
                                     @Nonnull final TopologyEntity entityB,
                                     @Nonnull final StringBuilder builder,
                                     @Nonnull final FormatRecommendation format) {
        final TopologyEntityDTO a = entityA.getTopologyEntityImpl().toProto();
        final TopologyEntityDTO b = entityB.getTopologyEntityImpl().toProto();

        if (a.equals(b)) {
            // No diff to append
            if (verbosity == Verbosity.COMPLETE_VERBOSITY) {
                builder.append(builder.append("  Entity:\n")
                    .append(gson.toJson(a))
                    .append("\n"));
                return true;
            } else {
                return false;
            }
        }

        final String aJson = gson.toJson(a);
        final String bJson = gson.toJson(b);

        try {
            int initialSpaces = format == FormatRecommendation.PRETTY ? 2 : 1;
            final DiffOutput difference = differ.semanticDiff(aJson, bJson, verbosity, format, initialSpaces);
            if (shouldRecord(difference)) {
                if (format == FormatRecommendation.PRETTY) {
                    builder.append("  Entity:\n");
                }
                builder.append(difference.diff);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            logger.error("Error diffing JSON object {} against {}", aJson, bJson);
            logger.error("Diffing exception: ", e);
            return false;
        }
    }
}
