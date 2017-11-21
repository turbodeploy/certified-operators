package com.vmturbo.topology.processor.topology.pipeline;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;

import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A {@link TopologyPipeline} captures the different stages required to build and broadcast
 * a topology out of the topology processor. {@link TopologyPipeline}s are built in the
 * {@link TopologyPipelineFactory}.
 * <p>
 * The pipeline consists of a set of {@link Stage}s. The output of one stage becomes the input
 * to the next {@link Stage}. There is some (minimal) shared state between the stages, represented
 * by the {@link TopologyPipelineContext}. The stages are executed one at a time. In the future
 * we can add parallel execution of certain subsets of the pipeline, but that's not necessary
 * at the time of this writing (Nov 2017).
 *
 * @param <PipelineInput> The input to the pipeline. This is the input to the first stage.
 * @param <PipelineOutput> The output of the pipeline. This is the output of the last stage.
 */
public class TopologyPipeline<PipelineInput, PipelineOutput> {

    private static final String TOPOLOGY_TYPE_LABEL = "topology_type";

    private static final String PIPELINE_STAGE_LABEL = "stage";

    /**
     * This metric tracks the total duration of a topology broadcast (i.e. all the stages
     * in the pipeline).
     */
    private static final DataMetricSummary TOPOLOGY_BROADCAST_SUMMARY = DataMetricSummary.builder()
            .withName("tp_broadcast_duration_seconds")
            .withHelp("Duration of a topology broadcast.")
            .withLabelNames(TOPOLOGY_TYPE_LABEL)
            .build()
            .register();

    private static final DataMetricSummary TOPOLOGY_STAGE_SUMMARY = DataMetricSummary.builder()
            .withName("tp_broadcast_pipeline_duration_seconds")
            .withHelp("Duration of the individual stages in a topology broadcast.")
            .withLabelNames(TOPOLOGY_TYPE_LABEL, PIPELINE_STAGE_LABEL)
            .build()
            .register();

    private static final Logger LOGGER = LogManager.getLogger();

    private final List<Stage> stages;

    private final TopologyPipelineContext context;

    private TopologyPipeline(@Nonnull final List<Stage> stages,
                             @Nonnull final TopologyPipelineContext context) {
        this.stages = stages;
        this.context = context;
    }

    /**
     * Run the stages of the pipeline, in the order they're configured.
     *
     * @param input The input to the pipeline.
     * @return The output of the pipeline.
     * @throws TopologyPipelineException If a required stage fails.
     * @throws InterruptedException If the pipeline is interrupted.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public PipelineOutput run(@Nonnull PipelineInput input)
            throws TopologyPipelineException, InterruptedException {
        LOGGER.info("Running the topology pipeline for context {}",
                context.getTopologyInfo().getTopologyContextId());
        Object curStageInput = input;
        try (DataMetricTimer pipelineTimer =
                 TOPOLOGY_BROADCAST_SUMMARY.labels(context.getTopologyTypeName()).startTimer()) {
            for (final Stage stage : stages) {
                try (final DataMetricTimer stageTimer =
                         TOPOLOGY_STAGE_SUMMARY.labels(context.getTopologyTypeName(),
                                 getSnakeCaseName(stage)).startTimer()) {
                    // TODO (roman, Nov 13) OM-27195: Consider refactoring the builder and pipeline
                    // into more of a linked-list format so that there is better type safety.
                    LOGGER.info("Executing stage {}", stage.getClass().getSimpleName());
                    curStageInput = stage.execute(curStageInput);
                } catch (PipelineStageException | RuntimeException e) {
                    final String message = "Topology pipeline failed at stage " +
                            stage.getClass().getSimpleName() + " with error: " + e.getMessage();
                    // Put a log message here as well, just in case the caller
                    // of the pipeline forgets to log the exception.
                    LOGGER.error(message, e);
                    throw new TopologyPipelineException(message, e);
                }
            }
        }
        LOGGER.info("Topology pipeline for context {} finished successfully.",
                context.getTopologyInfo().getTopologyContextId());
        return (PipelineOutput)curStageInput;
    }

    private static String getSnakeCaseName(@Nonnull final Stage stage) {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
                stage.getClass().getSimpleName());
    }

    /**
     * A passthrough stage is a pipeline stage where the type of input and output is the same.
     * If the input is mutable, it may change during the stage. The stage may also not change
     * the input, but perform some operations based on the input (e.g. recording the input somewhere).
     *
     * @param <InputType> The type of the input.
     */
    public abstract static class PassthroughStage<InputType> extends Stage<InputType, InputType> {
        /**
         * @return whether or not the stage is required. If a required stage fails, the pipeline
         * fails. If a non-required stage fails, the next stage gets the input of the passthrough
         * stage.
         */
        protected boolean required() {
            return false;
        }

        @Nonnull
        @Override
        public InputType execute(@Nonnull InputType input) throws PipelineStageException {
            try {
                passthrough(input);
            } catch (PipelineStageException e) {
                if (!required()) {
                    LOGGER.warn("Non-required pipeline stage {} failed with error: {}",
                            getClass().getSimpleName(), e.getMessage());
                } else {
                    throw e;
                }
            } catch (RuntimeException e) {
                if (!required()) {
                    LOGGER.warn("Non-required pipeline stage {} failed with error: {}",
                            getClass().getSimpleName(), e.getMessage());
                } else {
                    throw new PipelineStageException(e);
                }
            }
            return input;
        }

        public abstract void passthrough(InputType input) throws PipelineStageException;
    }

    /**
     * A pipeline stage takes an input and produces an output that gets passed along to the
     * next stage.
     *
     * @param <StageInput> The type of the input.
     * @param <StageOutput> The type of the output.
     */
    public abstract static class Stage<StageInput, StageOutput> {

        private TopologyPipelineContext context;

        /**
         *
         * @param input The input.
         * @return The output of the stage.
         * @throws PipelineStageException If there is an error executing this stage.
         * @throws InterruptedException If the stage is interrupted.
         */
        @Nonnull
        public abstract StageOutput execute(@Nonnull StageInput input)
                throws PipelineStageException, InterruptedException;

        @VisibleForTesting
        void setContext(@Nonnull final TopologyPipelineContext context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        protected TopologyPipelineContext getContext() {
            return context;
        }
    }

    /**
     * An exception indicating that a {@link TopologyPipeline} failed (due to a failure in
     * a required stage).
     */
    public static class TopologyPipelineException extends Exception {
        public TopologyPipelineException(@Nonnull final String error, @Nonnull final Throwable cause) {
            super(error, cause);
        }
    }

    /**
     * An exception thrown when a stage of the pipeline fails.
     */
    public static class PipelineStageException extends Exception {
        public PipelineStageException(@Nonnull final Throwable cause) {
            super(cause);
        }

        public PipelineStageException(@Nonnull final String error) {
            super(error);
        }
    }

    public static <PipelineInput, PipelineOutput> Builder<PipelineInput, PipelineOutput, PipelineInput>
            newBuilder(@Nonnull final TopologyPipelineContext context) {
        return new Builder<>(context);
    }

    /**
     * A builder class for a {@link TopologyPipeline}.
     *
     * @param <PipelineInput> The initial input type. This will be the input type to the entire
     *                        pipeline.
     * @param <PipelineOutput> The output of the pipeline.
     * @param <NextStageInput> The input type required for the next stage in the pipeline. This
     *                   changes when the {@link Builder#addStage(Stage)} method is called in order
     *                   to check that the pipeline stages are compatible at compile time.
     */
    public static class Builder<PipelineInput, PipelineOutput, NextStageInput> {

        private final List<Stage> stages;

        private final TopologyPipelineContext context;

        public Builder(@Nonnull final TopologyPipelineContext context) {
            this.stages = new LinkedList<>();
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        public <NextStageOutput> Builder<PipelineInput, PipelineOutput, NextStageOutput> addStage(
                @Nonnull final Stage<NextStageInput, NextStageOutput> stage) {
            Objects.requireNonNull(stage);
            stage.setContext(context);
            stages.add(stage);
            // Cast to reinterpret the generic variables. This is safe at runtime
            // because generic parameters don't affect runtime :)
            return (Builder<PipelineInput, PipelineOutput, NextStageOutput>)this;
        }

        @Nonnull
        public TopologyPipeline<PipelineInput, PipelineOutput> build() {
            return new TopologyPipeline<>(stages, context);
        }
    }
}
