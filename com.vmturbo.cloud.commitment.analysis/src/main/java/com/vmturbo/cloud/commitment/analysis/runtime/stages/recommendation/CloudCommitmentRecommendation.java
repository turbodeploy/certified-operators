package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.time.Period;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A cloud commitment recommendation as output from a {@link RecommendationAnalysisTask}.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudCommitmentRecommendation {


    /**
     * A unique identifier of this recommendation.
     * @return The ID of the recommendation.
     */
    long recommendationId();

    /**
     * The recommendation info, containing relevant info to the cloud commitment type (e.g.
     * purchasing region for an RI).
     * @return The recommendation info. The contained info varies by cloud commitment type.
     */
    @Nonnull
    RecommendationInfo recommendationInfo();

    /**
     * Info containing the covered demand for this recommendation.
     * @return Info containing the covered demand for this recommendation.
     */
    @Nonnull
    CoveredDemandInfo coveredDemandInfo();

    /**
     * The savings calculation result from the {@link CloudCommitmentSavingsCalculator}.
     * @return The savings calculation result.
     */
    @Auxiliary
    @Nonnull
    SavingsCalculationResult savingsCalculationResult();

    /**
     * The break even period calculated by the {@link BreakEvenCalculator}.
     *
     * @return The break even period for the cloud commitment.
     */
    Optional<Period> breakEven();

    @Derived
    default boolean isActionable() {
        return savingsCalculationResult().recommendation().recommendationQuantity() > 0;
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link CloudCommitmentRecommendation} instances.
     */
    class Builder extends ImmutableCloudCommitmentRecommendation.Builder {}
}
