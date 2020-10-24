package com.vmturbo.cloud.commitment.analysis.runtime;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.CloudCommitmentInventoryResolverStage.CloudCommitmentInventoryResolverStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.RecommendationSpecMatcherStage.RecommendationSpecMatcherStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationStage.CoverageCalculationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.pricing.PricingResolverStage.PricingResolverStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisStage.RecommendationAnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval.DemandRetrievalStage.DemandRetrievalFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStage.DemandTransformationFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * A factory class for creating instances of {@link AnalysisPipeline}.
 */
public class AnalysisPipelineFactory {

    private final IdentityProvider identityProvider;

    private final InitializationStageFactory initializationStageFactory;

    private final DemandRetrievalFactory demandRetrievalFactory;

    private final DemandClassificationFactory demandClassificationFactory;

    private final DemandTransformationFactory demandTransformationFactory;

    private final CloudCommitmentInventoryResolverStageFactory inventoryResolverStageFactory;

    private final CoverageCalculationFactory coverageCalculationFactory;

    private final RecommendationSpecMatcherStageFactory recommendationSpecMatcherStageFactory;

    private final PricingResolverStageFactory pricingResolverStageFactory;

    private final RecommendationAnalysisFactory recommendationAnalysisFactory;

    /**
     * Constructs an analysis pipeline factory.
     * @param identityProvider The identity provider, used to assign IDs to individual stages.
     * @param initializationStageFactory The {@link InitializationStageFactory} to create the first
     *                                   stage of analysis.
     * @param demandRetrievalFactory The {@link DemandRetrievalFactory} to create the second stage
     *                               of analysis.
     * @param demandClassificationFactory The {@link DemandClassificationFactory} to create the followup
     *                                    stage to demand selection.
     * @param demandTransformationFactory The {@link DemandTransformationFactory} to create the demand
     *                                    transformation stage.
     * @param inventoryResolverStageFactory The {@link CloudCommitmentInventoryResolverStageFactory} to
     *                                    create the CloudCommitmentInventoryResolverStage.
     * @param coverageCalculationFactory  The {@link CoverageCalculationFactory} to create the coverage
     *                                    calculation stage after commitment inventory resolution.
     * @param recommendationSpecMatcherStageFactory The {@link RecommendationSpecMatcherStageFactory} to
     *                                    create the spec matcher recommendation stage.
     * @param pricingResolverStageFactory The {@link PricingResolverStageFactory} to create the pricing
     *                                    resolver stage after the spec recommendation stage.
     * @param recommendationAnalysisFactory  The {@link RecommendationAnalysisFactory} to create the
     *                                       recommendation analysis stage.
     */
    public AnalysisPipelineFactory(@Nonnull IdentityProvider identityProvider,
                                   @Nonnull InitializationStageFactory initializationStageFactory,
                                   @Nonnull DemandRetrievalFactory demandRetrievalFactory,
                                   @Nonnull DemandClassificationFactory demandClassificationFactory,
                                   @Nonnull DemandTransformationFactory demandTransformationFactory,
                                   @Nonnull CloudCommitmentInventoryResolverStageFactory inventoryResolverStageFactory,
                                   @Nonnull CoverageCalculationFactory coverageCalculationFactory,
                                   @Nonnull RecommendationSpecMatcherStageFactory recommendationSpecMatcherStageFactory,
                                   @Nonnull PricingResolverStageFactory pricingResolverStageFactory,
                                   @Nonnull RecommendationAnalysisFactory recommendationAnalysisFactory) {

        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.initializationStageFactory = Objects.requireNonNull(initializationStageFactory);
        this.demandRetrievalFactory = Objects.requireNonNull(demandRetrievalFactory);
        this.demandClassificationFactory = Objects.requireNonNull(demandClassificationFactory);
        this.demandTransformationFactory = Objects.requireNonNull(demandTransformationFactory);
        this.inventoryResolverStageFactory = Objects.requireNonNull(inventoryResolverStageFactory);
        this.coverageCalculationFactory = Objects.requireNonNull(coverageCalculationFactory);
        this.recommendationSpecMatcherStageFactory = Objects.requireNonNull(recommendationSpecMatcherStageFactory);
        this.pricingResolverStageFactory = Objects.requireNonNull(pricingResolverStageFactory);
        this.recommendationAnalysisFactory = Objects.requireNonNull(recommendationAnalysisFactory);
    }

    /**
     * Constructs a new {@link AnalysisPipeline} instance, based on the provided config and context.
     *
     * @param analysisConfig The analysis config.
     * @param analysisContext The analysis context.
     * @return A new {@link AnalysisPipeline} instance.
     */
    @Nonnull
    public AnalysisPipeline createAnalysisPipeline(@Nonnull CloudCommitmentAnalysisConfig analysisConfig,
                                                   @Nonnull CloudCommitmentAnalysisContext analysisContext) {

        Preconditions.checkNotNull(analysisConfig);

        return AnalysisPipeline.newBuilder()
                .addStage(initializationStageFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(demandRetrievalFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(demandClassificationFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(demandTransformationFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(inventoryResolverStageFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(coverageCalculationFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(recommendationSpecMatcherStageFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(pricingResolverStageFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .addStage(recommendationAnalysisFactory.createStage(
                        identityProvider.next(),
                        analysisConfig,
                        analysisContext))
                .build();
    }
}
