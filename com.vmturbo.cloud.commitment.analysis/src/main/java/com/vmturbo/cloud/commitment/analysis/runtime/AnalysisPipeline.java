package com.vmturbo.cloud.commitment.analysis.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

/**
 * Represents the set of {@link AnalysisStage} instances that must be run in sequential order as part
 * of a pipeline.
 */
public class AnalysisPipeline {

    private final List<AnalysisStage> stages;


    private AnalysisPipeline(@Nonnull List<AnalysisStage> stages) {
        this.stages = ImmutableList.copyOf(Objects.requireNonNull(stages));
    }

    /**
     * The list of stages, which are linked based on IO. The output of the first stage is expected to
     * be supported by the input of the second stage, who's output should be used as the input to the
     * third stage, etc.
     *
     * @return The immutable list of stages.
     */
    @Nonnull
    public List<AnalysisStage> stages() {
        return stages;
    }

    /**
     * Creates a new {@link Builder} instance.
     * @return A newly created builder instance.
     */
    @Nonnull
    public static Builder<Void> newBuilder() {
        return new Builder();
    }

    /**
     * A builder class for {@link AnalysisPipeline}.
     * @param <NextStageInputT> The expected input for the next stage passed to {@link Builder#addStage(AnalysisStage)}.
     */
    public static class Builder<NextStageInputT> {

        private final List<AnalysisStage> stages = new ArrayList<>();

        /**
         * Add a new stage to the end of the pipeline.
         *
         * @param stage The {@link AnalysisStage} to add.
         * @param <NewStageOutputT> The output of {@code stage}, which will be used as the expected
         *                        input for the next stage.
         * @return This {@link Builder} instance for method chaining.
         */
        @Nonnull
        public <NewStageOutputT> Builder<NewStageOutputT> addStage(
                @Nonnull AnalysisStage<NextStageInputT, NewStageOutputT> stage) {

            stages.add(Objects.requireNonNull(stage));

            return (Builder<NewStageOutputT>)this;
        }

        /**
         * Creates the configured {@link AnalysisPipeline} instance.
         * @return The newly built {@link AnalysisPipeline} instance.
         */
        @Nonnull
        public AnalysisPipeline build() {
            return new AnalysisPipeline(stages);
        }
    }
}
