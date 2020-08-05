package com.vmturbo.cloud.commitment.analysis.runtime;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.IdentityProvider;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.selection.DemandSelectionStage.DemandSelectionFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * A factory class for creating instances of {@link AnalysisPipeline}.
 */
public class AnalysisPipelineFactory {

    private final IdentityProvider identityProvider;

    private final InitializationStageFactory initializationStageFactory;

    private final DemandSelectionFactory demandSelectionFactory;

    private final DemandClassificationFactory demandClassificationFactory;

    /**
     * Constructs an analysis pipeline factory.
     * @param identityProvider The identity provider, used to assign IDs to individual stages.
     * @param initializationStageFactory The {@link InitializationStageFactory} to create the first
     *                                   stage of analysis.
     * @param demandSelectionFactory The {@link DemandSelectionFactory} to create the second stage
     *                               of analysis.
     * @param demandClassificationFactory The {@link DemandClassificationFactory} to create the followup
     *                                    stage to demand selection.
     */
    public AnalysisPipelineFactory(@Nonnull IdentityProvider identityProvider,
                                   @Nonnull InitializationStageFactory initializationStageFactory,
                                   @Nonnull DemandSelectionFactory demandSelectionFactory,
                                   @Nonnull DemandClassificationFactory demandClassificationFactory) {

        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.initializationStageFactory = Objects.requireNonNull(initializationStageFactory);
        this.demandSelectionFactory = Objects.requireNonNull(demandSelectionFactory);
        this.demandClassificationFactory = Objects.requireNonNull(demandClassificationFactory);
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
                        demandSelectionFactory.createStage(
                                identityProvider.next(),
                                analysisConfig,
                                analysisContext))
                .addStage(
                        demandClassificationFactory.createStage(
                                identityProvider.next(),
                                analysisConfig,
                                analysisContext))
                .build();
    }
}
