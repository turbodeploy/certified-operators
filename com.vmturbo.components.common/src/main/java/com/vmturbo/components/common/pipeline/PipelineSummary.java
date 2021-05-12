package com.vmturbo.components.common.pipeline;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.stringtemplate.v4.ST;

import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.utils.TimeUtil;

/**
 * The summary of {@link Pipeline}, intended to be a quick way to visualize what went
 * right/wrong in a pipeline execution.
 *
 * <p/>The intended lifecycle of the summary is:
 * - Creation, before the pipeline runs.
 * - {@link PipelineSummary#start()} when the pipeline starts execution.
 * - {@link PipelineSummary#endStage(Status, boolean)} when a stage ends, with whatever status the
 *   stage ends with.
 * - {@link PipelineSummary#fail(String)} if the pipeline fails prematurely.
 *
 * <p/>Queries for the pipeline's current summary (via {@link PipelineSummary#toString()})
 * may come at any time in the lifecycle, so the summary should be thread-safe.
 */
@ThreadSafe
public abstract class PipelineSummary extends AbstractSummary {

    /**
     * The template used to visualize the summary as a string.
     */
    private static final String PIPELINE_SUMMARY_TEMPLATE =
            "======== Pipeline Summary ========\n"
            + "<preamble>\n"
            + "\n"
            + "Status: <status>\n"
            + "Current Stage: <curStage>\n"
            + "\n"
            + "Pipeline Started: <startTime>\n"
            + "Pipeline Completed: <endTime>\n"
            + "Pipeline Duration (s): <duration>\n"
            + "\n"
            + "======== Stage Breakdown ========\n"
            + "<stages;separator=\"-------\\n\">"
            + "=================================\n";

    /**
     * Set exactly once - when the pipeline starts executing.
     */
    private volatile Instant pipelineStart = null;

    /**
     * Set exactly once - when the last stage of the pipeline completes.
     */
    private volatile Instant pipelineEnd = null;

    protected PipelineSummary(@Nonnull final Clock clock,
                            @Nonnull final List<Stage> stages) {
        super(clock, stages);
    }

    /**
     * To be called when the pipeline starts running.
     */
    @Override
    public synchronized void start() {
        super.start();
        this.pipelineStart = clock.instant();
    }

    @Override
    protected void end(@Nonnull final Optional<String> errorMessage) {
        super.end(errorMessage);
        this.pipelineEnd = clock.instant();
    }

    protected abstract String getPreamble();

    @Override
    protected String renderToString() {
        final boolean completed = pipelineEnd != null;
        final ST template = new ST(PIPELINE_SUMMARY_TEMPLATE);
        template.add("preamble", getPreamble());
        template.add("curStage", getCurStageSummary()
            .map(summary -> summary.stage.getName()).orElse("None"));
        template.add("startTime", pipelineStart.toString());
        template.add("endTime", completed ? pipelineEnd.toString() : "Still running");
        template.add("duration", TimeUtil.humanReadable(Duration.between(pipelineStart,
            completed ? pipelineEnd : clock.instant())));
        template.add("stages", statusForStage);

        if (getErrorMessage().isPresent()) {
            template.add("status", "FAILED: " + getErrorMessage().get());
        } else if (completed) {
            template.add("status", "COMPLETED");
        } else {
            template.add("status", "RUNNING");
        }
        return template.render();
    }

    /**
     * The summary of a particular stage in a {@link Pipeline}.
     *
     * <p/>The intended lifecycle of the summary is:
     * - Creation
     * - {@link StageSummary#start()} when the stage starts execution.
     * - {@link StageSummary#end(Status)} when the stage ends, with whatever status the
     *   stage ends with.
     */
    @ThreadSafe
    public static class StageSummary {

        /**
         * The template used to visualize the summary as a string.
         */
        private static final String STAGE_SUMMARY_TEMPLATE =
            "<stageName> --- <stageStatus>\n"
            + NESTING_SPACING + "<message>\n";

        /**
         * The clock used to create start and end times.
         */
        private final Clock clock;

        /**
         * The stage being summarized.
         */
        private final Stage stage;

        /**
         * Set exactly once - when the stage starts.
         */
        private volatile Instant startTime = null;

        /**
         * Set exactly once - when the stage ends.
         */
        private volatile Instant endTime = null;

        /**
         * Set exactly once - when the stage ends.
         */
        private volatile Status status = null;

        StageSummary(@Nonnull final Clock clock,
                     @Nonnull final Stage stage ) {
            this.clock = clock;
            this.stage = Objects.requireNonNull(stage);
        }

        /**
         * To be called when the stage starts executing.
         */
        public synchronized void start() {
            startTime = clock.instant();
        }

        /**
         * To be called when the stage finishes executing.
         *
         * @param status The status of the stage on exit.
         */
        public synchronized void end(@Nonnull final Status status) {
            this.status = Objects.requireNonNull(status);
            endTime = clock.instant();
        }

        @Override
        public synchronized String toString() {
            final boolean completed = this.status != null;
            final ST template = new ST(STAGE_SUMMARY_TEMPLATE)
                    .add("stageName", stage.getName())
                    .add("stageStatus", status());
            if (completed) {
                template.add("message", StringUtils.strip(this.status.getMessageWithSummary()));
            } else {
                // Fill in template with a blank string to avoid error message.
                template.add("message", "");
            }
            return template.render();
        }

        /**
         * Return the human-readable form of the current status for the stage.
         * @return The stage status.
         */
        @Nonnull
        private synchronized String status() {
            if (this.status == null) {
                if (this.startTime == null) {
                    return "NOT STARTED";
                } else {
                    return "RUNNING FOR " + TimeUtil.humanReadable(
                        Duration.between(startTime, clock.instant()));
                }
            } else {
                final Duration duration = Duration.between(startTime, endTime);
                switch (this.status.getType()) {
                    case SUCCEEDED:
                        return "SUCCEEDED in " + TimeUtil.humanReadable(duration);
                    case FAILED:
                        return "FAILED after " + TimeUtil.humanReadable(duration);
                    case WARNING:
                        return "WARNING (completed in "
                            + TimeUtil.humanReadable(Duration.between(startTime, endTime)) + ")";
                    default:
                        return "ILLEGAL STATUS: " + this.status.getType()
                            + " after " + TimeUtil.humanReadable(duration);
                }
            }
        }
    }
}
