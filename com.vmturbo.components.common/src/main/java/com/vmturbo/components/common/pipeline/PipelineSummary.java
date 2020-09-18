package com.vmturbo.components.common.pipeline;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

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
 * - {@link PipelineSummary#endStage(Status)} when a stage ends, with whatever status the
 *   stage ends with.
 * - {@link PipelineSummary#fail(String)} if the pipeline fails prematurely.
 *
 * <p/>Queries for the pipeline's current summary (via {@link PipelineSummary#toString()})
 * may come at any time in the lifecycle, so the summary should be thread-safe.
 */
@ThreadSafe
public abstract class PipelineSummary {

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
     * Constant to indicate an "unset" index. It's -1 because -1 is always an illegal index.
     */
    private static final int UNSET_STAGE_IDX = -1;

    /**
     * The clock used to determine start/end times.
     */
    private final Clock clock;

    /**
     * {@link StageSummary} objects, in the order that the stages appear in the pipeline.
     */
    private final List<StageSummary> statusForStage;

    /**
     * The index of the currently executing stage. This is initially set to
     * {@link PipelineSummary.UNSET_STAGE_IDX}, to 0 when the pipeline starts executing,
     * and only moves "forward" as stages complete.
     */
    private volatile int curStageIdx;

    /**
     * Set exactly once - when the pipeline starts executing.
     */
    private volatile Instant pipelineStart = null;

    /**
     * Set exactly once - when the last stage of the pipeline completes.
     */
    private volatile Instant pipelineEnd = null;

    /**
     * The error message of the pipeline, if it terminated with an error.
     */
    private volatile Optional<String> errorMessage = Optional.empty();

    protected PipelineSummary(@Nonnull final Clock clock,
                            @Nonnull final List<Stage> stages) {
        this.clock = clock;
        statusForStage = stages.stream()
                .map(stage -> new StageSummary(clock, stage))
                .collect(Collectors.toList());
        this.curStageIdx = UNSET_STAGE_IDX;
    }

    /**
     * To be called when the pipeline starts running.
     */
    public synchronized void start() {
        this.curStageIdx = 0;
        this.pipelineStart = clock.instant();
        this.statusForStage.get(curStageIdx).start();
    }

    /**
     * To be called when the current stage in the pipeline ends.
     * Because stages in the {@link Pipeline} are arranged sequentially and run immediately
     * after the other, there is no explicit method to mark the beginning of a stage, or the end
     * of the pipeline. When one stage ends, the next stage is started. When the final stage ends,
     * the pipeline ends.
     *
     * @param stageStatus The status returned by the completed stage.
     */
    public synchronized void endStage(@Nonnull final Status stageStatus) {
        if (curStageIdx == UNSET_STAGE_IDX) {
            throw new IllegalStateException("End Stage called before start of pipeline.");
        } else if (curStageIdx >= statusForStage.size()) {
            throw new IllegalStateException("End Stage call after end of pipeline.");
        }

        statusForStage.get(curStageIdx).end(stageStatus);
        curStageIdx++;
        if (curStageIdx < statusForStage.size()) {
            statusForStage.get(curStageIdx).start();
        } else {
            end(Optional.empty());
        }
    }

    /**
     * Called if the pipeline fails before completion.
     *
     * @param message The error message explaining the failure.
     */
    public synchronized void fail(@Nonnull final String message) {
        end(Optional.of(message));
    }

    private void end(@Nonnull final Optional<String> errorMessage) {
        this.curStageIdx = UNSET_STAGE_IDX;
        this.pipelineEnd = clock.instant();
        this.errorMessage = errorMessage;
    }

    protected abstract String getPreamble();

    @Override
    public synchronized String toString() {
        final boolean completed = pipelineEnd != null;
        final ST template = new ST(PIPELINE_SUMMARY_TEMPLATE);
        template.add("preamble", getPreamble());
        template.add("curStage", curStageIdx == UNSET_STAGE_IDX
            ? "None" : statusForStage.get(curStageIdx).stage.getName());
        template.add("startTime", pipelineStart.toString());
        template.add("endTime", completed ? pipelineEnd.toString() : "Still running");
        template.add("duration", TimeUtil.humanReadable(Duration.between(pipelineStart,
            completed ? pipelineEnd : clock.instant())));
        template.add("stages", statusForStage);

        if (errorMessage.isPresent()) {
            template.add("status", "FAILED: " + errorMessage.get());
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
            + "    <message>\n";

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
