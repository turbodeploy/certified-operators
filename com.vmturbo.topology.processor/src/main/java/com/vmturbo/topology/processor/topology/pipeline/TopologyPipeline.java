package com.vmturbo.topology.processor.topology.pipeline;

import java.time.Clock;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A {@link TopologyPipeline} captures the different stages required to build and broadcast
 * a topology out of the topology processor. {@link TopologyPipeline}s are built in the
 * {@link LivePipelineFactory} for realtime pipelines, and {@link PlanPipelineFactory} for
 * plan pipelines.
 *
 * <p>The pipeline consists of a set of {@link Stage}s. The output of one stage becomes the input
 * to the next {@link Stage}. There is some (minimal) shared state between the stages, represented
 * by the {@link TopologyPipelineContext}. The stages are executed one at a time. In the future
 * we can add parallel execution of certain subsets of the pipeline, but that's not necessary
 * at the time of this writing (Nov 2017).
 *
 * @param <PipelineInput> The input to the pipeline. This is the input to the first stage.
 * @param <PipelineOutput> The output of the pipeline. This is the output of the last stage.
 */
public class TopologyPipeline<PipelineInput, PipelineOutput> {

    private static final int MAX_STAGE_RETRIES = 3;

    private static final long MAX_STAGE_RETRY_INTERVAL_MS = 30_000;

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

    private static final Logger logger = LogManager.getLogger();

    private final List<Stage> stages;

    private final TopologyPipelineContext context;

    private final TopologyPipelineSummary pipelineSummary;

    private TopologyPipeline(@Nonnull final List<Stage> stages,
                             @Nonnull final TopologyPipelineContext context) {
        this.stages = stages;
        this.context = context;
        this.pipelineSummary = new TopologyPipelineSummary(Clock.systemUTC(), context, stages);
    }

    /**
     * Get the {@link TopologyInfo} describing the topology being constructed by this pipeline.
     * This returns the current version of the {@link TopologyInfo}. Some stages may edit the
     * {@link TopologyInfo}, and those changes may or may not be present in the snapshot depending
     * on whether the stage has run or not. To get the guaranteed "final" version of the
     * {@link TopologyInfo}, call this method only after {@link TopologyPipeline#run(Object)}.
     *
     * @return The {@link TopologyInfo} describing the topology constructed by this pipeline.
     */
    @Nonnull
    public TopologyInfo getTopologyInfo() {
        return context.getTopologyInfo();
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
        logger.info("Running the topology pipeline for context {}",
                context.getTopologyInfo().getTopologyContextId());
        pipelineSummary.start();
        Object curStageInput = input;
        try (DataMetricTimer pipelineTimer =
                 TOPOLOGY_BROADCAST_SUMMARY.labels(context.getTopologyTypeName()).startTimer()) {
            for (final Stage stage : stages) {
                try (final DataMetricTimer stageTimer =
                         TOPOLOGY_STAGE_SUMMARY.labels(context.getTopologyTypeName(),
                                 getSnakeCaseName(stage)).startTimer()) {
                    // TODO (roman, Nov 13) OM-27195: Consider refactoring the builder and pipeline
                    // into more of a linked-list format so that there is better type safety.
                    logger.info("Executing stage {}", stage.getClass().getSimpleName());
                    final StageResult result = stage.execute(curStageInput);
                    curStageInput = result.getResult();
                    pipelineSummary.endStage(result.status());
                } catch (PipelineStageException | RuntimeException e) {
                    final String message = "Topology pipeline failed at stage " +
                            stage.getClass().getSimpleName() + " with error: " + e.getMessage();
                    pipelineSummary.fail(message);
                    logger.info("\n{}", pipelineSummary);
                    throw new TopologyPipelineException(message, e);
                }
            }
        }
        logger.info("\n{}", pipelineSummary);
        logger.info("Topology pipeline for context {} finished successfully.",
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

        /**
         * If the stage should be re-tried after a particular exception,
         * get the number of ms to wait before retrying the stage.
         *
         * Note - if the stage is to be retried, it is the stage writer's responsibility to
         * make sure it's idemnpotent (i.e. re-running the stage should be safe).
         *
         * @param numAttempts The number of attempts so far. Starts at 1.
         * @param e The exception encountered on this attempt.
         * @return An {@link Optional} containing the ms to wait before retrying. Empty optional if
         *         no retry.
         */
        @Nonnull
        protected Optional<Long> getRetryIntervalMs(final int numAttempts, final Exception e) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public StageResult<InputType> execute(@Nonnull InputType input) throws PipelineStageException {
            Status status = null;
            boolean retry = false;
            int numAttempts = 0;
            Exception terminatingException = null;
            do {
                retry = false;
                numAttempts++;
                try {
                    status = passthrough(input);
                    break;
                } catch (PipelineStageException | RuntimeException e) {
                    // If the stage configured some kind of retry behaviour, we should respect
                    // it (unless it's been retrying too much!).
                    final Optional<Long> retryIntervalOpt = getRetryIntervalMs(numAttempts, e);
                    if (numAttempts <= MAX_STAGE_RETRIES && retryIntervalOpt.isPresent()) {
                        retry = true;
                        final long retryDelayMs =
                            Math.max(0, Math.min(retryIntervalOpt.get(), MAX_STAGE_RETRY_INTERVAL_MS));
                        logger.warn("Pipeline stage {} failed with transient error. " +
                                "Retrying after {}ms. Error: {}",
                            getClass().getSimpleName(), retryDelayMs, e.getMessage());
                        try {
                            Thread.sleep(retryDelayMs);
                        } catch (InterruptedException e1) {
                            terminatingException = e1;
                        }
                    } else {
                        terminatingException = e;
                    }
                }
            } while (retry);

            // We should terminate with either an exception or a status.
            Preconditions.checkArgument(terminatingException != null || status != null);

            if (terminatingException != null) {
                if (terminatingException instanceof PipelineStageException) {
                    if (!required()) {
                        status = Status.failed("Error: " + terminatingException.getLocalizedMessage());
                        logger.warn("Non-required pipeline stage {} failed with error: {}",
                            getClass().getSimpleName(), terminatingException.getMessage());
                    } else {
                        throw (PipelineStageException) terminatingException;
                    }
                } else {
                    if (!required()) {
                        status = Status.failed("Runtime Error: " + terminatingException.getLocalizedMessage());
                        String stageName = getClass().getSimpleName();
                        if (terminatingException instanceof StatusRuntimeException) {
                            // we don't want to print the entire stack trace when it is a grpc exception
                            // as it will pollute the logs with unnecessary details.
                            logger.warn("Non-required pipeline stage {} failed with grpcError: {}",
                                stageName, terminatingException.getMessage());
                        } else {
                            logger.warn("Non-required pipeline stage {} failed with runtime Error: {}",
                                stageName, terminatingException.getMessage(), terminatingException);
                        }
                    } else {
                        throw new PipelineStageException(terminatingException);
                    }
                }
            }

            return StageResult.withResult(input)
                    .andStatus(status);
        }

        @Nonnull
        public abstract Status passthrough(InputType input) throws PipelineStageException;
    }

    /**
     * The output of a {@link Stage}. Essentially a semantically meaningful pair.
     *
     * @param <OutputType> The output type of the associated {@link Stage}.
     */
    public static class StageResult<OutputType> {

        private final OutputType result;

        private final Status status;

        /**
         * Use {@link StageResult#withResult(Object)}.
         */
        private StageResult(@Nonnull final Status status, @Nonnull final OutputType outputType) {
            this.status = Objects.requireNonNull(status);
            this.result = Objects.requireNonNull(outputType);
        }

        @Nonnull
        public OutputType getResult() {
            return result;
        }

        /**
         * @return The status the stage completed with.
         */
        @Nonnull
        public Status status() {
            return status;
        }

        /**
         * Factory method to start the construction of a {@link StageResult}.
         *
         * @param result The result of running the {@link Stage}.
         * @param <OutputType> See {@link StageResult}.
         * @return A {@link Builder} to complete the construction.
         */
        public static <OutputType> Builder<OutputType> withResult(@Nonnull final OutputType result) {
            return new Builder<>(result);
        }

        public static class Builder<OutputType> {
            private final OutputType result;

            private Builder(@Nonnull final OutputType result) {
                this.result = result;
            }

            /**
             * Complete the construction of a {@link StageResult} by setting the status the stage
             * completed with.
             *
             * @param status The status.
             * @return The {@link StageResult}.
             */
            @Nonnull
            public StageResult<OutputType> andStatus(@Nonnull final Status status) {
                return new StageResult<>(status, result);
            }
        }
    }

    /**
     * The status of a completed pipeline stage.
     */
    public static class Status {

        /**
         * The type of the status.
         */
        public enum Type {
            /**
             * Succeeded - the stage did what it's supposed to do with no major hiccups.
             */
            SUCCEEDED,

            /**
             * Warning - the stage encountered some errors, but the errors were not fatal in
             * the sense that there was no significant impact on the resulting topology or
             * the system.
             *
             * For example, if there are no discovered groups and we fail to upload discovered
             * groups due to a connection error, that might be a WARNING status instead of
             * FAILED.
             */
            WARNING,

            /**
             * Failed - the stage crashed and burned miserably. There should be no FAILED
             * statuses in the pipeline.
             *
             * This status means that the stage failed in a way that would have a non-trivial impact
             * on the resulting topology.
             */
            FAILED;
        }

        private Type type;

        private String message;

        private Status(@Nonnull final String message,
                       final Type type) {
            this.type = type;
            this.message = message;
        }

        /**
         * @return The message the stage completed with. May be empty if the stage completed
         *         successfully with no message.
         */
        @Nonnull
        public String getMessage() {
            return message;
        }

        /**
         * @return The status type.
         */
        public Type getType() {
            return type;
        }

        /**
         * The stage failed. See {@link Type.FAILED}.
         *
         * @param message The message to display to summarize the failure.
         * @return The failure status.
         */
        public static Status failed(@Nonnull final String message) {
            return new Status(message, Type.FAILED);
        }

        /**
         * The stage succeeded. When possible, use {@link Status#success(String)} instead to
         * communicate useful information about what happened during the stage
         * (e.g. modified 5 entities).
         *
         * @return The successful status.
         */
        public static Status success() {
            return new Status("", Type.SUCCEEDED);
        }

        /**
         * The stage succeeded.
         *
         * @param message A message describing what happened during the stage. This could be
         *                purely informative, or contain relatively harmless errors that occurred
         *                during the process. Use {@link Status#withWarnings(String)} if a stage
         *                completed with serious warnings/errors.
         * @return The successful status.
         */
        public static Status success(@Nonnull final String message) {
            return new Status(message, Type.SUCCEEDED);
        }

        /**
         * The stage suceeded - i.e. it didn't fail - but there were serious issues during the
         * stage execution that may dramatically affect the result of the stage or the result
         * of the pipeline.
         *
         * @param message A summary of the warning. This should be no longer than a few lines.
         * @return The warning status.
         */
        public static Status withWarnings(@Nonnull final String message) {
            return new Status(message, Type.WARNING);
        }
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
        public abstract StageResult<StageOutput> execute(@Nonnull StageInput input)
                throws PipelineStageException, InterruptedException;

        @VisibleForTesting
        void setContext(@Nonnull final TopologyPipelineContext context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        protected TopologyPipelineContext getContext() {
            return context;
        }

        @Nonnull
        public String getName() {
            return getClass().getSimpleName();
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

        public PipelineStageException(@Nonnull final String error, @Nullable final Throwable cause) {
            super(error, cause);
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
