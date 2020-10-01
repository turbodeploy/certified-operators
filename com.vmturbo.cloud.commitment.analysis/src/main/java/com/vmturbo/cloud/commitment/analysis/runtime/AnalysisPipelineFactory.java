package com.vmturbo.cloud.commitment.analysis.runtime;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.CloudCommitmentInventoryResolverStage.CloudCommitmentInventoryResolverStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
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

    private final CloudCommitmentInventoryResolverStageFactory cloudCommitmentInventoryResolverStageFactory;

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
     * @param cloudCommitmentInventoryResolverStageFactory The {@link CloudCommitmentInventoryResolverStageFactory} to
     *                                    create the CloudCommitmentInventoryResolverStage.
     */
    public AnalysisPipelineFactory(@Nonnull IdentityProvider identityProvider,
                                   @Nonnull InitializationStageFactory initializationStageFactory,
                                   @Nonnull DemandRetrievalFactory demandRetrievalFactory,
                                   @Nonnull DemandClassificationFactory demandClassificationFactory,
                                   @Nonnull DemandTransformationFactory demandTransformationFactory,
            @Nonnull CloudCommitmentInventoryResolverStageFactory cloudCommitmentInventoryResolverStageFactory) {

        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.initializationStageFactory = Objects.requireNonNull(initializationStageFactory);
        this.demandRetrievalFactory = Objects.requireNonNull(demandRetrievalFactory);
        this.demandClassificationFactory = Objects.requireNonNull(demandClassificationFactory);
        this.demandTransformationFactory = Objects.requireNonNull(demandTransformationFactory);
        this.cloudCommitmentInventoryResolverStageFactory = Objects.requireNonNull(cloudCommitmentInventoryResolverStageFactory);
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
                .addStage(
                        demandRetrievalFactory.createStage(
                                identityProvider.next(),
                                analysisConfig,
                                analysisContext))
                .addStage(
                        demandClassificationFactory.createStage(
                                identityProvider.next(),
                                analysisConfig,
                                analysisContext))
                .addStage(
                        demandTransformationFactory.createStage(
                                identityProvider.next(),
                                analysisConfig,
                                analysisContext))
                .addStage(
                        cloudCommitmentInventoryResolverStageFactory.createStage(
                                identityProvider.next(),
                                analysisConfig,
                                analysisContext))
                .build();
    }
}
