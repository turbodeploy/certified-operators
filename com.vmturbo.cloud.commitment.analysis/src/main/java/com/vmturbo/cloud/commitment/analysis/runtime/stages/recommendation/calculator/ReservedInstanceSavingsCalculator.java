package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator;

import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.AggregateDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.CloudTierDemandInfo;
import com.vmturbo.cloud.common.data.ImmutableTimeSeries;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.cloud.common.data.TimeSeriesData;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.util.FuzzyDouble;
import com.vmturbo.cloud.common.util.MutableFuzzyDouble;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;

/**
 * A {@link CloudCommitmentSavingsCalculator} implementation for reserved instances.
 */
public class ReservedInstanceSavingsCalculator implements CloudCommitmentSavingsCalculator {

    private final Logger logger = LogManager.getLogger();

    private final FuzzyDouble minimumSavingsPercentage;

    private final FuzzyDouble maximumCoveragePercentage;

    private ReservedInstanceSavingsCalculator(double minimumSavingsPercentage,
                                              double maximumCoveragePercentage) {

        Preconditions.checkArgument(minimumSavingsPercentage >= 0.0 && minimumSavingsPercentage <= 100.0,
                "Minimum savings over on-demand must be between 0.0 and 100.0");
        Preconditions.checkArgument(maximumCoveragePercentage >= 0.0 && maximumCoveragePercentage <= 100.0,
                "Maximum coverage percentage must be between 0.0 and 100.0");

        this.minimumSavingsPercentage = FuzzyDouble.newFuzzy(minimumSavingsPercentage);
        this.maximumCoveragePercentage = FuzzyDouble.newFuzzy(maximumCoveragePercentage);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public SavingsCalculationResult calculateSavingsRecommendation(
            @Nonnull final SavingsCalculationContext calculationContext) {

        logger.debug("Running RI savings calculation analysis (Tag={})", calculationContext.tag());

        final SavingsCalculation savingsCalculation = this.new SavingsCalculation(calculationContext);
        return savingsCalculation.calculateSavings();
    }

    /**
     * Represents a single RI savings calculation.
     */
    private class SavingsCalculation {

        private final SavingsCalculationContext calculationContext;

        private final TimeSeries<PrioritizedDemandSegment> rateSortedDemandSegments;

        private SavingsCalculation(@Nonnull SavingsCalculationContext calculationContext) {
            this.calculationContext = Objects.requireNonNull(calculationContext);

            // For savings calculations, demand needs to be sorted by the normalized rate. This is so that
            // the analysis can calculate potential savings assuming the worst case, in which the demand
            // with the lowest normalized rate is covered first.
            this.rateSortedDemandSegments = calculationContext.demandSegments().stream()
                    .map(demandSegment -> {
                        final NavigableMap<CloudTierDemandInfo, MutableAggregateDemand> demandMap =
                                demandSegment.tierDemandMap()
                                        .entrySet()
                                        .stream()
                                        .collect(ImmutableSortedMap.toImmutableSortedMap(
                                                Comparator.comparing(CloudTierDemandInfo::normalizedOnDemandRate)
                                                        .thenComparing(demandInfo ->
                                                                demandInfo.cloudTierDemand().cloudTierOid())
                                                        .thenComparing(CloudTierDemandInfo::hashCode),
                                                Map.Entry::getKey,
                                                (e) -> e.getValue().asMutable()));


                        return PrioritizedDemandSegment.builder()
                                .timeInterval(demandSegment.timeInterval())
                                .tierDemandMap(demandMap)
                                .build();
                    }).collect(ImmutableTimeSeries.toImmutableTimeSeries());
        }

        private SavingsCalculationResult calculateSavings() {

            if (logger.isDebugEnabled()) {
                logger.debug("Analysis demand info (Tag={}): {}",
                        calculationContext.tag(),
                        rateSortedDemandSegments.stream()
                                .map(PrioritizedDemandSegment::tierDemandMap)
                                .map(Map::keySet)
                                .flatMap(Set::stream)
                                .collect(ImmutableSet.toImmutableSet()));
            }

            reserveDemand();

            return SavingsCalculationResult.builder()
                    .recommendationContext(calculationContext)
                    .recommendation(calculateRecommendation())
                    .build();
        }

        private void reserveDemand() {

            Preconditions.checkArgument(maximumCoveragePercentage.isLessThanEq(100.0)
                    && maximumCoveragePercentage.isGreaterThanEq(0.0));

            if (maximumCoveragePercentage.isLessThan(100.0)) {

                logger.debug("Reserving demand for analysis (Tag={}, Max Coverage={})",
                        calculationContext.tag(), maximumCoveragePercentage.value());
                rateSortedDemandSegments.forEach(this::reserveSegmentDemand);
            } else {
                logger.debug("Skipping demand reservation (Tag={})", calculationContext.tag());
            }
        }

        @Nonnull
        private void reserveSegmentDemand(@Nonnull PrioritizedDemandSegment demandSegment) {

            final MutableFuzzyDouble aggregateDemandReduction =
                    maximumCoveragePercentage.subtractFrom(100.0)
                            .dividedBy(100.0)
                            .times(demandSegment.totalDemand())
                            .asMutableFuzzy();

            // Iterate the demand info for the segment from most expensive to cheapest.
            // TODO ejf (explain)
            final Iterator<CloudTierDemandInfo> demandInfoIterator = demandSegment.tierDemandMap()
                    .navigableKeySet()
                    .descendingIterator();

            while (aggregateDemandReduction.isPositive() && demandInfoIterator.hasNext()) {

                final CloudTierDemandInfo demandInfo = demandInfoIterator.next();
                final MutableAggregateDemand aggregateDemand = demandSegment.tierDemandMap().get(demandInfo);

                final double demandReservation = Math.min(
                        aggregateDemandReduction.value(),
                        aggregateDemand.uncoveredDemand());

                aggregateDemand.setReservedDemand(demandReservation);
                aggregateDemandReduction.subtract(demandReservation);
            }
        }

        private SavingsCalculationRecommendation calculateRecommendation() {

            final SortedSet<Long> candidateQuantities = calculatePotentialRecommendationQuantities();
            final Iterator<Long> quantitiesIterator = candidateQuantities.iterator();

            RecommendationAppraisal acceptedRecommendation =
                    appraiseRecommendation(0, 0).aggregateAppraisal();
            RecommendationAppraisal rejectedRecommendation = null;

            logger.debug("Analysis potential recommendation quantities (Tag={}): {}",
                    calculationContext.tag(), candidateQuantities);
            while (quantitiesIterator.hasNext() && rejectedRecommendation == null) {

                final long candidateQuantity = quantitiesIterator.next();
                final long netNewRecommendationAmount = candidateQuantity - acceptedRecommendation.recommendationQuantity();

                final RecommendationAppraisalResult appraisalResult =
                        appraiseRecommendation(candidateQuantity, netNewRecommendationAmount);
                final RecommendationAppraisal aggregateAppraisal = appraisalResult.aggregateAppraisal();
                final IncrementalAppraisal incrementalAppraisal = appraisalResult.incrementalAppraisal();

                final double savingsOverOnDemand = incrementalAppraisal.savingsOverOnDemand();
                if (minimumSavingsPercentage.isLessThanEq(savingsOverOnDemand)) {

                    logger.debug("Accepting recommendation appraisal (Tag={}): {}",
                            calculationContext.tag(), aggregateAppraisal);
                    acceptedRecommendation = aggregateAppraisal;
                } else {
                    rejectedRecommendation = aggregateAppraisal;
                }
            }

            return SavingsCalculationRecommendation.builder()
                    .from(acceptedRecommendation)
                    .rejectedRecommendation(Optional.ofNullable(rejectedRecommendation))
                    .build();
        }

        private RecommendationAppraisalResult appraiseRecommendation(long totalQuantity,
                                                                     long incrementalQuantity) {

            Preconditions.checkArgument(totalQuantity >= incrementalQuantity);

            // calculate aggregate appraisal stats
            final long totalCapacity = totalQuantity * rateSortedDemandSegments.size();
            final double totalRecommendationCost = totalCapacity * calculationContext.amortizedCommitmentRate();
            final MutableDouble usedCommitmentCapacity = new MutableDouble(0);
            final MutableDouble coveredDemand = new MutableDouble(0);
            final MutableDouble coveredOnDemandCost = new MutableDouble(0);
            final MutableDouble totalOnDemandCost = new MutableDouble(0);
            final MutableDouble inventoryCoveredDemand = new MutableDouble(0);
            final MutableDouble totalUncoveredDemand = new MutableDouble(0);
            final MutableDouble totalDemand = new MutableDouble(0);

            // calculate incremental appraisal stats
            final MutableDouble incrementalCoveredCost = new MutableDouble(0);
            final double incrementalRecommendationCost =
                    incrementalQuantity * rateSortedDemandSegments.size() * calculationContext.amortizedCommitmentRate();

            rateSortedDemandSegments.forEach(demandSegment -> {
                final MutableFuzzyDouble remainingCapacity = MutableFuzzyDouble.newFuzzy(totalQuantity);
                demandSegment.tierDemandMap().forEach((tierInfo, aggregateDemand) -> {
                    final double onDemandRate = tierInfo.normalizedOnDemandRate();
                    if (remainingCapacity.isPositive()) {

                        final double demandCommitmentRatio = tierInfo.demandCommitmentRatio();

                        final double recommendationCoverage = Math.min(
                                remainingCapacity.value() * demandCommitmentRatio,
                                aggregateDemand.coverableDemand());
                        final double usedRecommendationCapacity = recommendationCoverage / demandCommitmentRatio;

                        // record aggregate stats
                        coveredOnDemandCost.add(recommendationCoverage * onDemandRate);
                        coveredDemand.add(recommendationCoverage);
                        usedCommitmentCapacity.add(usedRecommendationCapacity);

                        // record incremental stats
                        final double incrementalUsedCapacity = remainingCapacity.isGreaterThan(incrementalQuantity)
                                ? usedRecommendationCapacity - (remainingCapacity.value() - incrementalQuantity)
                                : usedRecommendationCapacity;
                        if (incrementalUsedCapacity > 0) {
                            final double incrementalCoverage = incrementalUsedCapacity * tierInfo.demandCommitmentRatio();
                            incrementalCoveredCost.add(incrementalCoverage * onDemandRate);
                        }

                        remainingCapacity.subtract(usedRecommendationCapacity);
                    } // TODO (ejf) trace log remaining capacity

                    totalOnDemandCost.add(aggregateDemand.uncoveredDemand() * onDemandRate);
                    inventoryCoveredDemand.add(aggregateDemand.inventoryCoverage());
                    totalUncoveredDemand.add(aggregateDemand.uncoveredDemand());
                    totalDemand.add(aggregateDemand.totalDemand());
                });
            });

            final Duration appraisalDuration = Duration.between(
                    rateSortedDemandSegments.first().timeInterval().startTime(),
                    rateSortedDemandSegments.last().timeInterval().endTime());

            return RecommendationAppraisalResult.builder()
                    .aggregateAppraisal(RecommendationAppraisal.builder()
                            .appraisalDuration(appraisalDuration)
                            .recommendationQuantity(totalQuantity)
                            .recommendationUtilization(totalQuantity > 0
                                    ? (usedCommitmentCapacity.getValue() / totalCapacity) * 100
                                    : Double.NEGATIVE_INFINITY)
                            .coveredOnDemandCost(coveredOnDemandCost.getValue())
                            .aggregateOnDemandCost(totalOnDemandCost.getValue())
                            .recommendationCost(totalRecommendationCost)
                            .inventoryCoverage(totalDemand.getValue() > 0
                                    ? (inventoryCoveredDemand.getValue() / totalDemand.getValue()) * 100
                                    : Double.NEGATIVE_INFINITY)
                            .initialUncoveredDemand(totalUncoveredDemand.getValue())
                            .aggregateNormalizedDemand(totalDemand.getValue())
                            .coveredDemand(coveredDemand.getValue())
                            .recommendationCoverage(totalUncoveredDemand.getValue() > 0
                                    ? (coveredDemand.getValue() / totalUncoveredDemand.getValue()) * 100
                                    : Double.NEGATIVE_INFINITY)
                            .savings(coveredOnDemandCost.getValue() - totalRecommendationCost)
                            .savingsOverOnDemand(coveredOnDemandCost.getValue() > 0
                                    ? (coveredOnDemandCost.getValue() - totalRecommendationCost) / coveredOnDemandCost.getValue() * 100
                                    : Double.NEGATIVE_INFINITY)
                            .build())
                    .incrementalAppraisal(IncrementalAppraisal.builder()
                            .coveredOnDemandCost(incrementalCoveredCost.getValue())
                            .recommendationCost(incrementalRecommendationCost)
                            .savingsOverOnDemand(incrementalCoveredCost.getValue() > 0
                                    ? (incrementalCoveredCost.getValue() - incrementalRecommendationCost) / incrementalCoveredCost.getValue() * 100
                                    : Double.NEGATIVE_INFINITY)
                            .build())
                    .build();
        }

        /**
         * Determines the possible recommendation quantities to recommend, based on the demand. If
         * there are two demand segments, one with a demand quantity of 1, while the other has a demand
         * quantity of 4.5, this method will output quantities of (1,4,5). The analysis will skip over
         * considering quantities of 2 and 3, because they will have the same utilization (and therefore
         * the same savings over on-demand) as a quantity of 4.
         * @return The potential recommendation quantities to analyze.
         */
        private SortedSet<Long> calculatePotentialRecommendationQuantities() {

            return rateSortedDemandSegments.stream()
                    .map(demandSegment -> demandSegment.tierDemandMap().entrySet()
                            .stream()
                            .mapToDouble(demandEntry ->
                                    demandEntry.getValue().coverableDemand() / demandEntry.getKey().demandCommitmentRatio())
                            .reduce(0.0, Double::sum))
                    .flatMap(uncoveredDemand -> Stream.of(
                            (long)Math.floor(uncoveredDemand),
                            (long)Math.ceil(uncoveredDemand)))
                    .filter(recommendationQuantity -> recommendationQuantity > 0)
                    .collect(ImmutableSortedSet.toImmutableSortedSet(Ordering.natural()));
        }
    }

    /**
     * A factory class for creating {@link ReservedInstanceSavingsCalculator} instances.
     */
    public static class ReservedInstanceSavingsCalculatorFactory {

        /**
         * Constructs and returns a new RI savings calculator.
         * @param recommendationSettings The recommendation settings to consider in making RI recommendations.
         * @return The newly constructed RI savings calculator.
         */
        @Nonnull
        public ReservedInstanceSavingsCalculator newCalculator(@Nonnull RecommendationSettings recommendationSettings) {

            Preconditions.checkNotNull(recommendationSettings);

            return new ReservedInstanceSavingsCalculator(
                    recommendationSettings.getMinimumSavingsOverOnDemandPercent(),
                    recommendationSettings.getMaxDemandPercent());
        }
    }

    /**
     * A demand segment in which the tier demand map is sorted by the normalized rate.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface PrioritizedDemandSegment extends TimeSeriesData {

        /**
         * The tier demand map, sorted by the normalized rate (cheapest first).
         * @return The tier demand map, sorted by the normalized rate (cheapest first).
         */
        @Auxiliary
        NavigableMap<CloudTierDemandInfo, MutableAggregateDemand> tierDemandMap();

        /**
         * The total demand contained within this segment.
         * @return The total demand contained within this segment.
         */
        @Auxiliary
        @Derived
        default double totalDemand() {
            return tierDemandMap().values()
                    .stream()
                    .map(AggregateDemand::totalDemand)
                    .reduce(0.0, Double::sum);
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
         * A builder class for {@link PrioritizedDemandSegment} instances.
         */
        class Builder extends ImmutablePrioritizedDemandSegment.Builder {}
    }

    /**
     * The result of a recommendation appraisal analysis, representing an appraisal of the
     * full quantity and the step increment.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface RecommendationAppraisalResult {

        /**
         * The appraisal of the full recommendation quantity. The analysis works by analyzing incremental
         * additions to the recommended quantity. This appraisal will encapsulate the total
         * incremental amount.
         * @return The full appraisal amount.
         */
        @Nonnull
        RecommendationAppraisal aggregateAppraisal();

        /**
         * An appraisal of just the incremental amount being considered.
         * @return The incremental appraisal
         */
        @Nonnull
        IncrementalAppraisal incrementalAppraisal();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link RecommendationAppraisalResult} instances.
         */
        class Builder extends ImmutableRecommendationAppraisalResult.Builder {}
    }

    /**
     * An appraisal for a recommendation of an incremental commitment quantity.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface IncrementalAppraisal {

        /**
         * The on-demand cost covered by the commitment.
         * @return The on-demand cost covered by the commitment.
         */
        double coveredOnDemandCost();

        /**
         * The recommendation cost, relative to the quantity, commitment rate, and the number of
         * demand segments.
         * @return The recommendation cost.
         */
        double recommendationCost();

        /**
         * The savings over on-demand as a percentage up to 100.0.
         * @return The savings over on-demand.
         */
        double savingsOverOnDemand();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link IncrementalAppraisal} instances.
         */
        class Builder extends ImmutableIncrementalAppraisal.Builder {}
    }
}
