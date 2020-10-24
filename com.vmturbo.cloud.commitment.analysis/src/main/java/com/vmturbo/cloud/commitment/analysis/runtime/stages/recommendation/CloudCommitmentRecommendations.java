package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.util.List;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * The output of {@link RecommendationAnalysisStage}, representing all recommendations from the analysis.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudCommitmentRecommendations {

    /**
     * The source analysis topology for the recommendations.
     * @return The analysis topology.
     */
    @Nonnull
    AnalysisTopology analysisTopology();

    /**
     * The list of cloud commitment recommendations. The recommendations will include those in
     * which the analysis is recommending a quantity of zero.
     * @return The list of cloud commitment recommendations.
     */
    @Nonnull
    List<CloudCommitmentRecommendation> commitmentRecommendations();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link CloudCommitmentRecommendations} instances.
     */
    class Builder extends ImmutableCloudCommitmentRecommendations.Builder {}
}
