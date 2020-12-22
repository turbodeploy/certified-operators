package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.time.Period;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;

/**
 * The break even calculator is responsible for calculating the break even period for a CCA recommendation.
 * For example, for a CCA recommendation for a 1 year RI with 25% savings over on demand the brak even period
 * would be 9 months.
 */
public class BreakEvenCalculator {

    private final Logger logger = LogManager.getLogger();

    /**
     * Calculates the break even period for a Cloud Commitment in hours.
     * @param savingsCalculationResult The savings calculations result.
     * @param cloudCommitmentSpecData The cloud commitment spec data.
     *
     * @return An optional double representing the break even period.
     */
    public Optional<Period> calculateBreakEven(@Nonnull SavingsCalculationResult savingsCalculationResult,
                                                @Nonnull CloudCommitmentSpecData cloudCommitmentSpecData) {
        final SavingsCalculationRecommendation savingsCalculationRecommendation = savingsCalculationResult.recommendation();
        final double savings = savingsCalculationRecommendation.savingsOverOnDemand();
        if (savings < 0) {
            logger.debug("The savings for this recommendation is negative. The breakeven"
                    + " period for {} will not be set", savingsCalculationRecommendation);
            return Optional.empty();
        }
        final long termInHours = cloudCommitmentSpecData.termInHours();
        final double breakEvenPeriodInHours = (1 - savings / 100) * termInHours;
        return Optional.of(Period.ofDays((int)(breakEvenPeriodInHours / 24)));
    }
}
