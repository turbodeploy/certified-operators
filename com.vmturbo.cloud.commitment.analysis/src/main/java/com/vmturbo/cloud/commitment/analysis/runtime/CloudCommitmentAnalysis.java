package com.vmturbo.cloud.commitment.analysis.runtime;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage.StageResult;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * Represents a single analysis of cloud commitment recommendations, based on a provided configuration.
 * The analysis encapsulates the discrete steps (stages) required to complete the analysis.
 */
public class CloudCommitmentAnalysis {

    private static final String LOGGING_TAG = "analysisTag";

    private final Logger logger = LogManager.getLogger();

    private final CloudCommitmentAnalysisInfo analysisInfo;

    private final CloudCommitmentAnalysisContext analysisContext;

    private final CloudCommitmentAnalysisSummary analysisSummary;

    private final CloudCommitmentAnalysisConfig analysisConfig;

    private final AnalysisPipeline analysisPipeline;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);


    private CloudCommitmentAnalysis(@Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                                    @Nonnull CloudCommitmentAnalysisContext analysisContext,
                                    @Nonnull CloudCommitmentAnalysisConfig analysisConfig,
                                    @Nonnull AnalysisPipeline analysisPipeline) {

        this.analysisInfo = Objects.requireNonNull(analysisInfo, "Analysis info must be set");
        this.analysisContext = Objects.requireNonNull(analysisContext, "Analysis context must be set");
        this.analysisConfig = Objects.requireNonNull(analysisConfig, "Analysis config must be set");
        this.analysisPipeline = Objects.requireNonNull(analysisPipeline, "Analysis pipeline must be set");
        this.analysisSummary = new CloudCommitmentAnalysisSummary(
                analysisContext.getAnalysisInfo(), analysisConfig, analysisPipeline);
    }

    /**
     * Runs the cloud commitment analysis, blocking until completion. The analysis can only be run once.
     * Any subsequent invocation of {@link #run()} will have no effect.
     *
     * @throws CloudCommitmentAnalysisException A wrapped exception containing an exceptions thrown by
     * individual {@link AnalysisStage} instances.
     */
    public void run() throws CloudCommitmentAnalysisException {

        try (AnalysisExecutionContext aec = this.new AnalysisExecutionContext()) {
            if (!isStarted.getAndSet(true)) {
                logger.info("Cloud commitment analysis config:\n{}", analysisConfig);
                logger.info("Staring cloud commitment analysis:\n{}", analysisSummary);

                // The first stage is expected to accept null as input
                Object stageInput = null;
                for (AnalysisStage stage : analysisPipeline.stages()) {
                    try {
                        logger.info("Executing stage [{}]", stage.stageName());

                        analysisSummary.onStageStart(stage);

                        final StageResult<?> stageResult;
                        try (StageExecutionContext sec = this.new StageExecutionContext(stage)) {
                            stageResult = stage.execute(stageInput);
                        }

                        analysisSummary.onStageCompletion(stage, stageResult);

                        logger.info("Stage completed [{}]. Stage result:\n{}",
                                stage.stageName(), stageResult.resultSummary());

                        stageInput = stageResult.output();

                    } catch (Exception e) {

                        final String message = "{} Cloud commitment analysis failure at stage "
                                + stage.stageName() + " with error: " + e.getMessage();

                        analysisSummary.onStageFailure(stage, message);
                        logger.info("{}", analysisSummary);
                        throw new CloudCommitmentAnalysisException(message, e);
                    }
                }

                logger.info("{}", analysisSummary);
                logger.info("Cloud commitment analysis completed successfully");
            }
        }
    }

    /**
     * The {@link CloudCommitmentAnalysisInfo} for this analysis.
     * @return The {@link CloudCommitmentAnalysisInfo} for this analysis.
     */
    @Nonnull
    public CloudCommitmentAnalysisInfo info() {
        return analysisInfo;
    }

    /**
     * Creates and returns a new instance of {@link Builder}.
     * @return A newly created instance of {@link Builder}.
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Provides a summary of this analysis.
     * @return A summary of this analysis.
     */
    @Override
    public String toString() {
        return String.format("%s", analysisSummary);
    }

    /**
     * The status of both a {@link CloudCommitmentAnalysis} and underlying {@link AnalysisStage} instances.
     */
    @Immutable
    public interface Status {

        /**
         * The possible states of both the {@link CloudCommitmentAnalysis} and {@link AnalysisStage}.
         */
        enum State {
            READY,
            RUNNING,
            FAILED,
            COMPLETED
        }

        /**
         * The state of the analysis/stage.
         * @return The state of the analysis/stage.
         */
        @Nonnull
        State state();

        /**
         * The status message for the analysis/stage.
         * @return The status message for the analysis/stage.
         */
        @Default
        @Nonnull
        default String message() {
            return StringUtils.EMPTY;
        }

        /**
         * Creates and returns a status instance in the ready state with no status message.
         * @return A status instance in the ready state with no status message.
         */
        @Nonnull
        static Status ready() {
            return ImmutableStatus.builder()
                    .state(State.READY)
                    .build();
        }

        /**
         * Creates and returns a status instance in the running state with no status message.
         * @return A status instance in the running state with no status message.
         */
        @Nonnull
        static Status running() {
            return ImmutableStatus.builder()
                    .state(State.RUNNING)
                    .build();
        }

        /**
         * Creates and returns a status instance in the completed state with the provided
         * {@code message}.
         * @param message The status message.
         * @return A status instance in the completed state with the provided {@code message}.
         */
        @Nonnull
        static Status complete(@Nonnull String message) {
            return ImmutableStatus.builder()
                    .state(State.COMPLETED)
                    .message(message)
                    .build();
        }

        /**
         * Creates and returns a status instance in the failed state with the provided
         * {@code message}.
         * @param message The status message.
         * @return A status instance in the failed state with the provided {@code message}.
         */
        @Nonnull
        static Status fail(@Nonnull String message) {
            return ImmutableStatus.builder()
                    .state(State.FAILED)
                    .message(message)
                    .build();
        }
    }

    /**
     * A wrapper around the execution of the analysis. Responsible for setting the analysis
     * tag for logging purposes.
     */
    private class AnalysisExecutionContext implements AutoCloseable {

        private final CloseableThreadContext.Instance loggingContext;

        AnalysisExecutionContext() {
            final String loggingTag = String.format("%s|%s",
                    analysisInfo.getOid(), analysisInfo.getAnalysisTag());
            loggingContext = CloseableThreadContext.put(LOGGING_TAG, loggingTag);
        }

        /**
         * Closes the analysis execution context.
         */
        @Override
        public void close() {
            loggingContext.close();
        }
    }

    /**
     * A wrapper around the execution of an analysis stage. Responsible for setting the analysis
     * tag for logging purposes.
     */
    private class StageExecutionContext implements AutoCloseable {

        private final CloseableThreadContext.Instance loggingContext;

        StageExecutionContext(@Nonnull AnalysisStage stage) {
            final String loggingTag = String.format("%s|%s|%s",
                    analysisInfo.getOid(),
                    analysisInfo.getAnalysisTag(),
                    stage.stageName());
            loggingContext = CloseableThreadContext.put(LOGGING_TAG, loggingTag);
        }

        /**
         * Closes the stage execution context.
         */
        @Override
        public void close() {
            loggingContext.close();
        }
    }

    /**
     * A builder class for {@link CloudCommitmentAnalysis}.
     */
    public static class Builder {

        private CloudCommitmentAnalysisInfo analysisInfo;

        private CloudCommitmentAnalysisContext analysisContext;

        private CloudCommitmentAnalysisConfig analysisConfig;

        private AnalysisPipeline analysisPipeline;

        /**
         * Set the {@link CloudCommitmentAnalysisInfo}.
         * @param analysisInfo The analysis info.
         * @return This {@link Builder} for method chaining.
         */
        @Nonnull
        public Builder analysisInfo(@Nonnull CloudCommitmentAnalysisInfo analysisInfo) {
            this.analysisInfo = analysisInfo;
            return this;
        }

        /**
         * Set the {@link CloudCommitmentAnalysisContext}.
         * @param analysisContext The analysis context.
         * @return This {@link Builder} for method chaining.
         */
        @Nonnull
        public Builder analysisContext(@Nonnull CloudCommitmentAnalysisContext analysisContext) {
            this.analysisContext = analysisContext;
            return this;
        }

        /**
         * Set the {@link CloudCommitmentAnalysisConfig}.
         * @param analysisConfig The analysis config.
         * @return This {@link Builder} for method chaining.
         */
        @Nonnull
        public Builder analysisConfig(@Nonnull CloudCommitmentAnalysisConfig analysisConfig) {
            this.analysisConfig = analysisConfig;
            return this;
        }

        /**
         * Set the {@link AnalysisPipeline}.
         * @param analysisPipeline The analysis pipeline.
         * @return This {@link Builder} for method chaining.
         */
        @Nonnull
        public Builder analysisPipeline(@Nonnull AnalysisPipeline analysisPipeline) {
            this.analysisPipeline = analysisPipeline;
            return this;
        }

        /**
         * Builds a new instance of {@link CloudCommitmentAnalysis}.
         * @return The newly created instance of {@link CloudCommitmentAnalysis}.
         */
        @Nonnull
        public CloudCommitmentAnalysis build() {
            return new CloudCommitmentAnalysis(analysisInfo, analysisContext, analysisConfig, analysisPipeline);
        }

    }

    /**
     * A wrapping exception for any exception thrown by an {@link AnalysisStage}.
     */
    public static class CloudCommitmentAnalysisException extends Exception {

        /**
         * Construct a new {@link CloudCommitmentAnalysisException}.
         * @param error The error message.
         * @param cause The wrapped exception.
         */
        public CloudCommitmentAnalysisException(@Nonnull final String error, @Nonnull final Throwable cause) {
            super(error, cause);
        }
    }
}
