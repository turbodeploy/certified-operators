package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.ReservedInstanceSavingsCalculator.ReservedInstanceSavingsCalculatorFactory;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * The savings calculator is responsible for taking in demand and an associated on demand rate,
 * along with a potential cloud commitment candidate and the commitment rate and generating
 * a corresponding recommendation. The recommendation indicates the quantity of the cloud commitment
 * candidate to recommend purchasing, based on the demand, pricing, and settings.
 */
public interface CloudCommitmentSavingsCalculator {

    /**
     * Calculates a cloud commitment recommendation based on the {@code demandContext}, containing both
     * pricing and demand data.
     * @param demandContext The demand context, containing both the recorded demand in scope and the rates
     *                      associated with the demand and cloud commitment to recommend.
     * @return A result, containing recommendation amount and demand statistics collected in generating
     * the recommendation.
     */
    @Nonnull
    SavingsCalculationResult calculateSavingsRecommendation(
            @Nonnull SavingsCalculationContext demandContext);

    /**
     * A factory class for producing {@link CloudCommitmentSavingsCalculator} instances.
     */
    interface CloudCommitmentSavingsCalculatorFactory {

        /**
         * Generates a new savings calculation, based on the target cloud commitment type and
         * the associated recommendation settings.
         * @param commitmentType The target cloud commitment type.
         * @param recommendationSettings The recommendation settings. Only those settings relevant to
         *                               {@code commitmentType} will be used.
         * @return The newly constructed savings calculator.
         */
        @Nonnull
        CloudCommitmentSavingsCalculator newCalculator(
                @Nonnull CloudCommitmentType commitmentType,
                @Nonnull RecommendationSettings recommendationSettings);
    }

    /**
     * The default implementation of {@link CloudCommitmentSavingsCalculatorFactory}. This is a wrapper
     * around commitment-specific factories.
     */
    class DefaultCloudCommitmentSavingsCalculatorFactory implements CloudCommitmentSavingsCalculatorFactory {

        private final ReservedInstanceSavingsCalculatorFactory riSavingsCalculatorFactory;

        /**
         * Constructs a new default savings calculator factory.
         * @param riSavingsCalculatorFactory The reserved instance savings calculator factory.
         */
        public DefaultCloudCommitmentSavingsCalculatorFactory(
                @Nonnull ReservedInstanceSavingsCalculatorFactory riSavingsCalculatorFactory) {
            this.riSavingsCalculatorFactory = Objects.requireNonNull(riSavingsCalculatorFactory);
        }

        /**
         * {@inheritDoc}.
         */
        @Override
        public CloudCommitmentSavingsCalculator newCalculator(
                @Nonnull CloudCommitmentType commitmentType,
                @Nonnull final RecommendationSettings recommendationSettings) {

            if (commitmentType == CloudCommitmentType.RESERVED_INSTANCE) {
                return riSavingsCalculatorFactory.newCalculator(recommendationSettings);
            } else {
                throw new UnsupportedOperationException(String.format(
                        "%s is not a supported commitment type.", commitmentType));
            }
        }
    }

    /**
     * The result of a savings calculation.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface SavingsCalculationResult {

        /**
         * The input context analyzed in generating this result.
         * @return The input recommendation context to the savings calculator.
         */
        @Nonnull
        SavingsCalculationContext recommendationContext();

        /**
         * the savings calculation recommendation, containing the quantity of the commitment
         * to recommend.
         * @return The savings calculation recommendation.
         */
        @Nonnull
        SavingsCalculationRecommendation recommendation();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link SavingsCalculationResult} instances.
         */
        class Builder extends ImmutableSavingsCalculationResult.Builder {}
    }

    /**
     * A savings calculation recommendation, encapsulating the calculator's appraisal of the
     * quantity recommended.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface SavingsCalculationRecommendation extends RecommendationAppraisal {

        /**
         * The closest potential recommendation appraised above the recommended quantity. There may not
         * be a rejected recommendation, if the recommended recommendation covers 100% of the
         * analyzed demand.
         * @return An optional containing the rejected recommendation appraisal directly after the
         * quantity contained within this recommendation.
         */
        Optional<RecommendationAppraisal> rejectedRecommendation();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link SavingsCalculationRecommendation} instances.
         */
        class Builder extends ImmutableSavingsCalculationRecommendation.Builder {}
    }

    /**
     * Represents an appraisal of a potential recommendation quantity by the savings calculator.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface RecommendationAppraisal {

        /**
         * The quantity of the cloud commitment to recommend purchasing. The quantity will be relative
         * to the cloud commitment rate set in the {@link SavingsCalculationContext}.
         * @return The recommendation quantity.
         */
        long recommendationQuantity();

        /**
         * Represents the total cost of all uncovered demand within the {@link SavingsCalculationContext}.
         * Uncovered demand is all demand not covered by inventory.
         * @return The aggregate on-demand cost as input to the savings calculation.
         */
        double aggregateOnDemandCost();

        /**
         * The on-demand cost that will be covered by this recommendation appraisal.
         * @return The on-demand cost that will be covered by this recommendation appraisal.
         */
        double coveredOnDemandCost();

        /**
         * The total cost of the recommendation, based on the quantity and cost contained within
         * the {@link SavingsCalculationContext}.
         * @return The total cost of the recommendation, based on the quantity and cost contained within
         * the {@link SavingsCalculationContext}.
         */
        double recommendationCost();

        /**
         * The coverage percentage from the inventory prior to any recommendation.
         * @return The coverage percentage from the inventory prior to any recommendation.
         */
        double inventoryCoverage();

        /**
         * The total demand contained within the {@link SavingsCalculationContext}. The demand is normalized
         * such that each demand set (pricing/tier) is relative to all others.
         * @return The total demand contained within the {@link SavingsCalculationContext}.
         */
        double aggregateNormalizedDemand();

        /**
         * The demand amount covered by the recommendation.
         * @return The demand amount covered by the recommendation.
         */
        double coveredDemand();

        /**
         * The uncovered demand prior to the recommendation. This will be the total demand minus the
         * coverage from commitment inventory.
         * @return The uncovered demand prior to the recommendation.
         */
        double initialUncoveredDemand();

        /**
         * The coverage percentage of all uncovered demand from {@link SavingsCalculationContext} by the
         * recommendation. For example, if there are 10 units of demand, the inventory is covering 2 and
         * the recommendation is calculated to cover another 4, the recommendation coverage would
         * be 50% (4/8).
         * @return the coverage percentage of all uncovered demand from {@link SavingsCalculationContext}
         * by the recommendation.
         */
        double recommendationCoverage();

        /**
         * The savings percentage over the covered on-demand cost. This is calculated as
         * ({@link #coveredOnDemandCost()} - {@link #recommendationCost()}) / {@link #coveredOnDemandCost()}.
         * @return The savings percentage over the covered on-demand cost. It is expected to be a value
         * between 0.0 and 100.0
         */
        double savingsOverOnDemand();

        /**
         * The savings from the recommendation, in the currency of provided rates. The savings is
         * the {@link #coveredOnDemandCost()} minus {@link #recommendationCost()}.
         * @return The savings from the recommendation, in the currency of provided rates.
         */
        double savings();

        /**
         * The utilization of the recommended quantity as a value between 0.0 and 100.0.
         * @return The utilization of the recommended quantity as a value between 0.0 and 100.0
         */
        double recommendationUtilization();

        /**
         * The duration of the analyzed demand segments.
         * @return The duration of the analyzed demand segments.
         */
        Duration appraisalDuration();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link RecommendationAppraisal} instances.
         */
        class Builder extends ImmutableRecommendationAppraisal.Builder {}
    }
}
