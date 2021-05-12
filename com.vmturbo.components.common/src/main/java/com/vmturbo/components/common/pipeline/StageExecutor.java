package com.vmturbo.components.common.pipeline;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Wraps the default implementation for the execution of a stage for pipelines and
 * pipeline segments that execute stages. Implementations include full {@link Pipeline}s
 * or {@link SegmentStage}s containing interior pipeline segments.
 *
 * @param <C> The type of the {@link PipelineContext}.
 */
public interface StageExecutor<C extends PipelineContext> {
    /**
     * Get the {@link PipelineContext} associated with this executor.
     *
     * @return the {@link PipelineContext} associated with this executor.
     */
    @Nonnull
    C getContext();

    /**
     * Get a {@link AbstractSummary} summarizing the status and performance of the
     * pipeline or segment's execution.
     *
     * @return a {@link AbstractSummary} summarizing the status and performance of the
     *         pipeline or segment's execution.
     */
    @Nonnull
    AbstractSummary getSummary();

    /**
     * Perform basic execution of a stage.
     * <p/>
     * Exception handling for any exceptions that may be thrown during execution of the stage should be
     * done by the caller (ie the Pipeline or PipelineSegment executing the stage).
     *
     * @param stage The stage to execute.
     * @param input The input to the stage.
     * @param <I> The type of the stage's input
     * @param <O> The type of the stage's output.
     * @return A result describing the stage's output.
     * @throws PipelineStageException If something goes wrong during the execution of the stage.
     * @throws InterruptedException If the stage is interrupted during its execution.
     */
    default <I, O> StageResult<O> runStage(@Nonnull final  Stage<I, O, C> stage, @Nonnull final I input)
        throws PipelineStageException, InterruptedException {
        final String stageName = stage.getSnakeCaseName();
        final C pipelineContext = getContext();

        try (DataMetricTimer stageTimer = pipelineContext.startStageTimer(stageName);
             TracingScope stageScope = pipelineContext.startStageTrace(stageName)) {
            // TODO (roman, Nov 13) OM-27195: Consider refactoring the builder and pipeline
            // into more of a linked-list format so that there is better type safety.
            final StageResult<O> result = stage.execute(input);
            result.getStatus().setContextMemberSummary(stage.contextMemberCleanup(pipelineContext));
            getSummary().endStage(result.getStatus(), true);

            return result;
        }
    }
}
