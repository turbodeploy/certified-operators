package com.vmturbo.cloud.commitment.analysis.runtime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * A discrete step to perform as part of a {@link CloudCommitmentAnalysis}. The stage is expected to
 * not have side effects, as it may be retried if it fails.
 * @param <StageInput> The input to stage execution.
 * @param <StageOutput> The output of stage execution.
 */
public interface AnalysisStage<StageInput, StageOutput> {

    /**
     * Executes the underlying stage logic.
     * @param input The input to the stage, from the preceding stage.
     * @return The output of the stage, passed to the succeeding stage.
     */
    @Nonnull
    StageResult<StageOutput> execute(StageInput input);

    /**
     * The ID of the stage, used to uniquely identify a stage implementation across all analyses.
     * @return The ID of the stage.
     */
    long id();

    /**
     * The stage name.
     * @return The stage name.
     */
    @Nonnull
    String stageName();

    /**
     * The stage result, containing the stage output and a human-readable summary of the result.
     * @param <StageOutput> The stage output type.
     */
    @Immutable
    interface StageResult<StageOutput> {

        /**
         * The output of the stage.
         * @return The output of the stage.
         */
        @Nullable
        StageOutput output();

        /**
         * A human-readable summary of the stage output.
         * @return A human-readable summary of the stage output.
         */
        @Default
        @Nonnull
        default String resultSummary() {
            return StringUtils.EMPTY;
        }
    }

    /**
     * A factory for creating a stage. Each stage is expected to implement a corresponding stage factory.
     * @param <StageInput> The input of the stage produced by the factory.
     * @param <StageOutput> The output of the stage produced by the factory.
     */
    interface StageFactory<StageInput, StageOutput> {

        /**
         * Create a new stage instance, based on the provided configuration. A new stage will be created for
         * each {@link CloudCommitmentAnalysis} instance.
         * @param id The unique ID of the stage.
         * @param config The configuration of the analysis.
         * @param context The context of the analysis, used to share context data across stages
         * @return The newly created stage.
         */
        @Nonnull
        AnalysisStage<StageInput, StageOutput> createStage(long id,
                                                           @Nonnull CloudCommitmentAnalysisConfig config,
                                                           @Nonnull CloudCommitmentAnalysisContext context);
    }
}
