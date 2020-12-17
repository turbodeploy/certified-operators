package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.time.Instant;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;

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
    public StageResult execute(final Object o) throws AnalysisConfigValidationException {

        validateAnalysisConfig();

        // First we need to truncate the requested look back start time based on the analysis interval.
        // For example, if we are analyzing demand per hour, we will truncate the look back start time
        // to the closest (earlier) hour.
        final Instant analysisStartTime = lookBackStartTime.truncatedTo(
                analysisContext.getAnalysisBucket().unit());
        final Instant analysisEndTime = Instant.now().truncatedTo(
                analysisContext.getAnalysisBucket().unit());
        final TimeInterval analysisWindow = TimeInterval.builder()
                .startTime(analysisStartTime)
                .endTime(analysisEndTime)
                .build();
        analysisContext.setAnalysisWindow(analysisWindow);

        return StageResult.builder()
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

    private void validateAnalysisConfig() throws AnalysisConfigValidationException {

        try {

            // Once recommendations on projected demand are supported, these checks will be updated
            // to require either allocated or projected demand selections.

            // validate the demand selection config
            final HistoricalDemandSelection demandSelection = analysisConfig.getDemandSelection();
            Preconditions.checkArgument(demandSelection.hasAllocatedSelection(),
                    "Demand selection must include allocated demand selection");

            //validate the purchase profile
            final CommitmentPurchaseProfile purchaseProfile = analysisConfig.getPurchaseProfile();
            Preconditions.checkArgument(purchaseProfile.hasAllocatedSelection(),
                    "Purchase profile must include allocated demand selection");

        } catch (IllegalArgumentException e) {
            throw new AnalysisConfigValidationException(e.getMessage());
        }
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

    /**
     * An exception indicating validation of the {@link CloudCommitmentAnalysisConfig} failed.
     */
    public static class AnalysisConfigValidationException extends Exception {

        /**
         * Constructs a new {@link AnalysisConfigValidationException} instance.
         * @param message The message of the exception.
         */
        public AnalysisConfigValidationException(String message) {
            super(message);
        }
    }
}
