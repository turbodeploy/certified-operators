package com.vmturbo.cloud.commitment.analysis.runtime;

import java.time.Instant;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.AnalysisContextFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * A factory class for creating {@link CloudCommitmentAnalysis}. THis class is responsible for also
 * creating (through factory classes) the {@link CloudCommitmentAnalysisContext} and
 * {@link AnalysisPipeline} for the analysis.
 */
public class AnalysisFactory {

    private final IdentityProvider identityProvider;
    private final AnalysisPipelineFactory analysisPipelineFactory;
    private final AnalysisContextFactory analysisContextFactory;

    /**
     * Construct an analysis factory.
     *
     * @param identityProvider The identity, used for assigning the analysis ID.
     * @param analysisPipelineFactory The analysis pipeline factory.
     * @param analysisContextFactory The analysis context factory.
     */
    public AnalysisFactory(@Nonnull IdentityProvider identityProvider,
                           @Nonnull AnalysisPipelineFactory analysisPipelineFactory,
                           @Nonnull AnalysisContextFactory analysisContextFactory) {

        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.analysisPipelineFactory = Objects.requireNonNull(analysisPipelineFactory);
        this.analysisContextFactory = Objects.requireNonNull(analysisContextFactory);
    }

    /**
     * Creates a {@link CloudCommitmentAnalysis} instance, based on the provided {@code analysisConfig}.
     *
     * @param analysisConfig The requested analysis configuration.
     * @return The newly created {@link CloudCommitmentAnalysis} instance.
     */
    @Nonnull
    public CloudCommitmentAnalysis createAnalysis(@Nonnull CloudCommitmentAnalysisConfig analysisConfig) {
        final CloudCommitmentAnalysisInfo analysisInfo = createAnalysisInfo(analysisConfig);
        final CloudCommitmentAnalysisContext analysisContext =
                analysisContextFactory.createContext(analysisInfo, analysisConfig);
        final AnalysisPipeline analysisPipeline = analysisPipelineFactory
                .createAnalysisPipeline(analysisConfig, analysisContext);

        return CloudCommitmentAnalysis.newBuilder()
                .analysisInfo(analysisInfo)
                .analysisConfig(analysisConfig)
                .analysisContext(analysisContext)
                .analysisPipeline(analysisPipeline)
                .build();

    }

    private CloudCommitmentAnalysisInfo createAnalysisInfo(@Nonnull CloudCommitmentAnalysisConfig analysisConfig) {

        return CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(identityProvider.next())
                .setAnalysisTag(analysisConfig.getAnalysisTag())
                .setCreationTime(Instant.now().toEpochMilli())
                .setAnalysisTopology(analysisConfig.getAnalysisTopology())
                .setTopologyReference(analysisConfig.getTopologyReference())
                .build();
    }
}
