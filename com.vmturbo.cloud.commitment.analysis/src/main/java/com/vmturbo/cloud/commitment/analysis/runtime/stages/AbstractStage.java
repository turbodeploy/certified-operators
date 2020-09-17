package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * An abstract implementation of {@link AnalysisStage}.
 * @param <StageInputT> The input type of the underlying stage.
 * @param <StageOutputT> The output type of the underlying stage.
 */
public abstract class AbstractStage<StageInputT, StageOutputT> implements AnalysisStage<StageInputT, StageOutputT> {

    protected final long id;

    protected final CloudCommitmentAnalysisContext analysisContext;

    protected final CloudCommitmentAnalysisConfig analysisConfig;


    protected AbstractStage(long id,
                            @Nonnull CloudCommitmentAnalysisConfig analysisConfig,
                            @Nonnull CloudCommitmentAnalysisContext analysisContext) {

        this.id = id;
        this.analysisConfig = Objects.requireNonNull(analysisConfig);
        this.analysisContext = Objects.requireNonNull(analysisContext);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long id() {
        return id;
    }
}
