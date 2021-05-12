package com.vmturbo.components.common.pipeline;

import java.time.Clock;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.stringtemplate.v4.ST;

import com.vmturbo.components.common.pipeline.Pipeline.Status;

/**
 * The summary of {@link SegmentStage}, intended to be a quick way to visualize what went
 * right/wrong in a pipeline segment.
 *
 * <p/>The intended lifecycle of the summary is:
 * - Creation, before the segment runs.
 * - {@link PipelineSummary#endStage(Status, boolean)} when a stage ends, with whatever status the
 *   stage ends with.
 *
 * <p/>Queries for the pipeline's current summary (via {@link SegmentSummary#toString()})
 * may come at any time in the lifecycle, so the summary should be thread-safe.
 */
@ThreadSafe
public class SegmentSummary extends AbstractSummary {
    /**
     * The template used to visualize the summary as a string.
     */
    private static final String SEGMENT_SUMMARY_TEMPLATE =
        "Status: <status>\n"
            + "\n"
            + "-------\n"
            + "<stages;separator=\"-------\\n\">";

    protected SegmentSummary(@Nonnull Clock clock, @Nonnull List<Stage> stages) {
        super(clock, stages);
    }

    @Override
    protected String renderToString() {
        final ST template = new ST(SEGMENT_SUMMARY_TEMPLATE);
        template.add("stages", statusForStage);

        if (getErrorMessage().isPresent()) {
            template.add("status", "FAILED: " + getErrorMessage().get());
        } else if (getCurStageSummary().isPresent()) {
            template.add("status", "RUNNING");
        } else {
            template.add("status", "COMPLETED");
        }
        return template.render();
    }
}
