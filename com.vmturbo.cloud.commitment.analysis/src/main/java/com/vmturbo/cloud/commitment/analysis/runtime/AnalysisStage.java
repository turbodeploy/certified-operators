package com.vmturbo.cloud.commitment.analysis.runtime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * A discrete step to perform as part of a {@link CloudCommitmentAnalysis}. The stage is expected to
 * not have side effects, as it may be retried if it fails.
 * @param <StageInputT> The input to stage execution.
 * @param <StageOutputT> The output of stage execution.
 */
public interface AnalysisStage<StageInputT, StageOutputT> {

    /**
     * Executes the underlying stage logic.
     * @param input The input to the stage, from the preceding stage.
     * @return The output of the stage, passed to the succeeding stage.
     * @throws Exception The underlying stage exception.
     */
    @Nonnull
    StageResult<StageOutputT> execute(StageInputT input) throws Exception;

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
     * @param <StageOutputT> The stage output type.
     */
    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Immutable
    interface StageResult<StageOutputT> {

        /**
         * The output of the stage.
         * @return The output of the stage.
         */
        @Nullable
        StageOutputT output();

        /**
         * A human-readable summary of the stage output.
         * @return A human-readable summary of the stage output.
         */
        @Default
        @Nonnull
        default String resultSummary() {
            return StringUtils.EMPTY;
        }

        /**
         * Constructs and returns a new builder instance.
         * @param <StageOutputT> The stage output expected.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static <StageOutputT> Builder<StageOutputT> builder() {
            return new Builder<StageOutputT>();
        }

        /**
         * A builder class for {@link StageResult} instances.
         * @param <StageOutputT> The stage output
         */
        class Builder<StageOutputT> extends ImmutableStageResult.Builder<StageOutputT> {}
    }

    /**
     * A factory for creating a stage. Each stage is expected to implement a corresponding stage factory.
     * @param <StageInputT> The input of the stage produced by the factory.
     * @param <StageOutputT> The output of the stage produced by the factory.
     */
    interface StageFactory<StageInputT, StageOutputT> {

        /**
         * Create a new stage instance, based on the provided configuration. A new stage will be created for
         * each {@link CloudCommitmentAnalysis} instance.
         * @param id The unique ID of the stage.
         * @param config The configuration of the analysis.
         * @param context The context of the analysis, used to share context data across stages
         * @return The newly created stage.
         */
        @Nonnull
        AnalysisStage<StageInputT, StageOutputT> createStage(long id,
                                                           @Nonnull CloudCommitmentAnalysisConfig config,
                                                           @Nonnull CloudCommitmentAnalysisContext context);
    }
}
