package com.vmturbo.components.common.pipeline;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.PipelineSummary.StageSummary;

/**
 * The summary of {@link Pipeline} or Segment, intended to be a quick way to visualize what went
 * right/wrong in a pipeline execution.
 *
 * <p/>The intended lifecycle of the summary is:
 * - Creation, before the pipeline runs.
 * - {@link AbstractSummary#start()} when the pipeline starts execution.
 * - {@link AbstractSummary#endStage(Status, boolean)} when a stage ends, with whatever status the
 *   stage ends with.
 * - {@link AbstractSummary#fail(String)} if the pipeline fails prematurely.
 *
 * <p/>Queries for the pipeline's current summary (via {@link AbstractSummary#toString()})
 * may come at any time in the lifecycle, so the summary should be thread-safe.
 */
@ThreadSafe
public abstract class AbstractSummary {
    /**
     * Constant to indicate an "unset" index. It's -1 because -1 is always an illegal index.
     */
    public static final int UNSET_STAGE_IDX = -1;

    /**
     * The clock used to determine start/end times.
     */
    protected final Clock clock;

    /**
     * {@link StageSummary} objects, in the order that the stages appear in the pipeline.
     */
    protected final List<StageSummary> statusForStage;

    /**
     * The index of the currently executing stage. This is initially set to
     * {@link AbstractSummary#UNSET_STAGE_IDX}, to 0 when the pipeline starts executing,
     * and only moves "forward" as stages complete.
     */
    private volatile int curStageIdx;

    /**
     * Spacing (4 whitespaces) for nesting information inside a summary.
     */
    public static final String NESTING_SPACING = "    ";

    /**
     * The error message of the pipeline, if it terminated with an error.
     */
    private volatile Optional<String> errorMessage = Optional.empty();

    protected AbstractSummary(@Nonnull final Clock clock, @Nonnull final List<Stage> stages) {
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
        this.statusForStage.get(curStageIdx).start();
    }

    /**
     * To be called when the current stage in the pipeline ends.
     * Because stages in the {@link Pipeline} are arranged sequentially and run immediately
     * after the other, there is no explicit method to mark the beginning of a stage, or the end
     * of the pipeline . When one stage ends, the next stage is started if continueToNext is true.
     * When the final stage ends, the pipeline ends.
     *
     * @param stageStatus The status returned by the completed stage.
     * @param continueToNext Whether to continue to the next stage after ending the current one.
     *                       If the stage is ending due to an error that will end the pipeline or segment,
     *                       set continueToNext to false.
     */
    public synchronized void endStage(@Nonnull final Status stageStatus,
                                      boolean continueToNext) {
        if (curStageIdx == UNSET_STAGE_IDX) {
            throw new IllegalStateException("End Stage called before start of pipeline or segment.");
        } else if (curStageIdx >= statusForStage.size()) {
            throw new IllegalStateException("End Stage call after end of pipeline or segment.");
        }

        statusForStage.get(curStageIdx).end(stageStatus);
        if (continueToNext) {
            curStageIdx++;
            if (curStageIdx < statusForStage.size()) {
                statusForStage.get(curStageIdx).start();
            } else {
                end(Optional.empty());
            }
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

    /**
     * Render to a string. Synchronized to ensure thread safety.
     * Subclasses implement the specifics of the summary details in {@link #renderToString()}.
     *
     * @return A string detailing the status of the pipeline.
     */
    @Override
    public synchronized String toString() {
        return renderToString();
    }

    /**
     * Mark the end of the pipeline or segment being summarized.
     *
     * @param errorMessage An optional message describing the error that caused the pipeline or segment to stop.
     *                     If the pipeline or segment completed normally, this parameter shoudld be
     *                     {@link Optional#empty()}.
     */
    protected synchronized void end(@Nonnull final Optional<String> errorMessage) {
        this.curStageIdx = UNSET_STAGE_IDX;
        this.errorMessage = errorMessage;
    }

    /**
     * Get the error message describing why the pipeline or segment stopped.
     * {@link Optional#empty()} indicates the pipeline has not been stopped or if it stopped,
     * it completed successfully.
     *
     * @return the error message describing why the pipeline or segment stopped.
     */
    protected synchronized Optional<String> getErrorMessage() {
        return errorMessage;
    }

    /**
     * Get the summary for the currently executing stage.
     *
     * @return the summary for the currently executing stage.
     */
    protected synchronized Optional<StageSummary> getCurStageSummary() {
        return curStageIdx == UNSET_STAGE_IDX
            ? Optional.empty()
            : Optional.of(statusForStage.get(curStageIdx));
    }

    /**
     * Render the summary to a String describing the operation of the pipeline or summary.
     *
     * @return a String describing the operation of the pipeline or summary.
     */
    protected abstract String renderToString();
}
