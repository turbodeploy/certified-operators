package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendations;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * An analysis stage responsible for persisting {@link CloudCommitmentRecommendations} to the appropriate
 * stores (or channels).
 */
public class ResultsPersistenceStage extends AbstractStage<CloudCommitmentRecommendations, CloudCommitmentRecommendations> {

    private static final String STAGE_NAME = "Results Persistence";

    private final Logger logger = LogManager.getLogger();

    private final CloudCommitmentRecommendationStore recommendationStore;

    private final ActionPlanBroadcast actionPlanBroadcast;

    private ResultsPersistenceStage(final long id,
                                    @Nonnull final CloudCommitmentAnalysisConfig analysisConfig,
                                    @Nonnull final CloudCommitmentAnalysisContext analysisContext,
                                    @Nonnull CloudCommitmentRecommendationStore recommendationStore,
                                    @Nonnull ActionPlanBroadcast actionPlanBroadcast) {

        super(id, analysisConfig, analysisContext);

        this.recommendationStore = recommendationStore;
        this.actionPlanBroadcast = actionPlanBroadcast;
    }

    /**
     * Persists the {@code recommendations} to the {@link CloudCommitmentRecommendationStore} and
     * broadcasts the recommendations. Recommendations will only be broadcast on successful storage.
     * @param recommendations The cloud commitment recommendations to store/broadcast.
     * @return The same recommendations as input.
     * @throws Exception An exception during persistence.
     */
    @Nonnull
    @Override
    public StageResult<CloudCommitmentRecommendations> execute(final CloudCommitmentRecommendations recommendations) throws Exception {

        final CloudCommitmentAnalysisInfo analysisInfo = analysisContext.getAnalysisInfo();

        if (analysisInfo.hasAnalysisTopology()) {
            logger.info("Persisting recommendation results");
            recommendationStore.persistRecommendations(
                    analysisContext.getAnalysisInfo(),
                    recommendations.commitmentRecommendations(),
                    analysisContext.getSourceCloudTopology());

            logger.info("Broadcasting action plan");
            actionPlanBroadcast.sendNotification(
                    analysisContext.getAnalysisInfo(), recommendations.commitmentRecommendations());
        } else {
            logger.warn("Analysis topology not set. Skipping results persistence");
        }

        return StageResult.<CloudCommitmentRecommendations>builder()
                .output(recommendations)
                .build();
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * A factory class for creating {@link ResultsPersistenceStage} instances.
     */
    public static class ResultsPersistenceFactory implements
            AnalysisStage.StageFactory<CloudCommitmentRecommendations, CloudCommitmentRecommendations> {

        private final CloudCommitmentRecommendationStore recommendationStore;

        private final ActionPlanBroadcast actionPlanBroadcast;

        /**
         * Constructs a new factory instance.
         * @param recommendationStore The {@link CloudCommitmentRecommendationStore}.
         * @param actionPlanBroadcast The {@link ActionPlanBroadcast}.
         */
        public ResultsPersistenceFactory(@Nonnull CloudCommitmentRecommendationStore recommendationStore,
                                         @Nonnull ActionPlanBroadcast actionPlanBroadcast) {

            this.recommendationStore = Objects.requireNonNull(recommendationStore);
            this.actionPlanBroadcast = Objects.requireNonNull(actionPlanBroadcast);
        }

        /**
         * Creates a new {@link ResultsPersistenceStage} instance.
         * @param id The unique ID of the stage.
         * @param config The configuration of the analysis.
         * @param context The context of the analysis, used to share context data across stages
         * @return The newly created {@link ResultsPersistenceStage} instance.
         */
        @Nonnull
        @Override
        public AnalysisStage<CloudCommitmentRecommendations, CloudCommitmentRecommendations> createStage(
                final long id,
                @Nonnull final CloudCommitmentAnalysisConfig config,
                @Nonnull final CloudCommitmentAnalysisContext context) {

            return new ResultsPersistenceStage(
                    id,
                    config,
                    context,
                    recommendationStore,
                    actionPlanBroadcast);
        }
    }
}
