package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * An abstract implementation of {@link AnalysisStage}.
 * @param <StageInput> The input type of the underlying stage.
 * @param <StageOutput> The output type of the underlying stage.
 */
public abstract class AbstractStage<StageInput, StageOutput> implements AnalysisStage<StageInput, StageOutput> {

    protected final long id;

    protected final CloudCommitmentAnalysisContext analysisContext;

    protected final CloudCommitmentAnalysisConfig analysisConfig;

    protected final String logPrefix;


    protected AbstractStage(long id,
                            @Nonnull CloudCommitmentAnalysisConfig analysisConfig,
                            @Nonnull CloudCommitmentAnalysisContext analysisContext) {

        this.id = id;
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
        this.analysisContext = Objects.requireNonNull(analysisContext);

        this.logPrefix = String.format("%s(%s)", analysisContext.getLogMarker(), this.stageName());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long id() {
        return id;
    }
}
