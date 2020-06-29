package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.time.Instant;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStageResult;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * The initialization stage of a {@link com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis},
 * responsible for any initialization required before analysis begins.
 */
public class InitializationStage extends AbstractStage {

    private static final String STAGE_NAME = "Initialization";

    private final Instant lookBackStartTime;

    /**
     * Construct an initialization stage instance for a specific analysis.
     * @param id THe unique ID of the stage.
     * @param config The analysis config.
     * @param context The analysis context.
     */
    public InitializationStage(long id,
                               @Nonnull final CloudCommitmentAnalysisConfig config,
                               @Nonnull final CloudCommitmentAnalysisContext context) {
        super(id, config, context);

        lookBackStartTime = Instant.ofEpochMilli(
                Objects.requireNonNull(config.getDemandSelection().getLookBackStartTime()));
    }

    /**
     * This stage is responsible for initializing any required data prior to analysis.
     *
     * @param o This parameter is ignored.
     * @return The stage result with a null output.
     */
    @Override
    public StageResult execute(final Object o) {


        // First we need to truncate the requested look back start time based on the analysis interval.
        // For example, if we are analyzing demand per hour, we will truncate the look back start time
        // to the closest (earlier) hour.
        final Instant analysisStartTime = lookBackStartTime.truncatedTo(analysisContext.getAnalysisSegmentUnit());
        analysisContext.setAnalysisStartTime(analysisStartTime);

        return ImmutableStageResult.builder()
                .output(null)
                .build();
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * A fatory class for creating {@link InitializationStage} instances.
     */
    public static class InitializationStageFactory implements AnalysisStage.StageFactory<Void, Void> {

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public AnalysisStage<Void, Void> createStage(final long id,
                                                     @Nonnull final CloudCommitmentAnalysisConfig config,
                                                     @Nonnull final CloudCommitmentAnalysisContext context) {

            return new InitializationStage(id, config, context);
        }
    }
}
