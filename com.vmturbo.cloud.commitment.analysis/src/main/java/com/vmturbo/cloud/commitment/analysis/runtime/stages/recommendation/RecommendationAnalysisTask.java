package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.time.Period;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationTopology.RecommendationDemandSegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.SavingsPricingResolver.SavingsPricingResolverFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.CloudCommitmentSavingsCalculatorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.AggregateDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.CloudTierDemandInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.DemandSegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * A single recommendation analysis (corresponding to a single potential buy action).
 */
public class RecommendationAnalysisTask implements Callable<CloudCommitmentRecommendation> {

    private final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final CloudCommitmentSavingsCalculator savingsCalculator;

    private final RecommendationTopology recommendationTopology;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private final RateAnnotatedCommitmentContext commitmentContext;

    private final SavingsPricingResolver pricingResolver;

    private final BreakEvenCalculator breakEvenCalculator;

    private RecommendationAnalysisTask(@Nonnull IdentityProvider identityProvider,
                                       @Nonnull CloudCommitmentSavingsCalculator savingsCalculator,
                                       @Nonnull SavingsPricingResolver pricingResolver,
                                       @Nonnull RecommendationTopology recommendationTopology,
                                       @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver) {

        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.savingsCalculator = Objects.requireNonNull(savingsCalculator);
        this.pricingResolver = Objects.requireNonNull(pricingResolver);
        this.recommendationTopology = Objects.requireNonNull(recommendationTopology);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.commitmentContext = recommendationTopology.commitmentContext();
        this.breakEvenCalculator = new BreakEvenCalculator();
    }

    /**
     * Runs the {@link CloudCommitmentSavingsCalculator} over the {@link RecommendationTopology} contained
     * within this task.
     *
     * @return The output of the {@link CloudCommitmentSavingsCalculator} calculation.
     * @throws Exception An unrecoverable exception.
     */
    @Override
    public CloudCommitmentRecommendation call() throws Exception {

        final Stopwatch stopwatch = Stopwatch.createStarted();

        final RecommendationInfo recommendationInfo = createRecommendationInfo();
        logger.debug("Starting recommendation analysis (Recommendation Info={}, Stopwatch={})",
                recommendationInfo, stopwatch);

        final SavingsCalculationContext savingsCalculationContext = createSavingsCalculationContext(recommendationInfo);
        logger.debug("Created savings calculation context (Recommendation Info={}, Stopwatch={})",
                recommendationInfo, stopwatch);

        final SavingsCalculationResult savingsCalculationResult =
                savingsCalculator.calculateSavingsRecommendation(savingsCalculationContext);

        final Optional<Period> breakEvenTime = breakEvenCalculator.calculateBreakEven(savingsCalculationResult, recommendationInfo.commitmentSpecData());
        logger.debug("Completed savings calculation (Recommendation Info={} Stopwatch={})",
                recommendationInfo, stopwatch);

        return CloudCommitmentRecommendation.builder()
                .recommendationId(identityProvider.next())
                .recommendationInfo(recommendationInfo)
                .coveredDemandInfo(createCoveredDemandInfo())
                .savingsCalculationResult(savingsCalculationResult)
                .breakEven(breakEvenTime)
                .build();
    }

    /**
     * Converts the {@link RecommendationTopology} to a {@link SavingsCalculationContext}.
     * @param recommendationInfo The recommendation info.
     * @return The savings calculation context.
     */
    @Nonnull
    private SavingsCalculationContext createSavingsCalculationContext(@Nonnull RecommendationInfo recommendationInfo) {

        final double amortizedCommitmentRate =
                commitmentContext.cloudCommitmentPricingData().amortizedHourlyRate();

        final TimeSeries<DemandSegment> demandSegmentSeries = recommendationTopology.demandSegments()
                .stream()
                .map(this::convertToDemandSegment)
                .collect(TimeSeries.toTimeSeries());

        return SavingsCalculationContext.builder()
                .amortizedCommitmentRate(amortizedCommitmentRate)
                .demandSegments(demandSegmentSeries)
                .tag(recommendationInfo.toString())
                .build();

    }

    private DemandSegment convertToDemandSegment(@Nonnull RecommendationDemandSegment demandSegment) {

        final Map<ScopedCloudTierInfo, CloudTierPricingData> tierPricingByScope =
                commitmentContext.cloudTierPricingByScope();

        // iterate over all cloud tier demand in scope of the recommendation, converting it to a more
        // abstract CloudTierDemandInfo/AggregateDemand pair. The savings calculator does not need to
        // know about demand scope or classifications (it assumes all demand within the savings context
        // is within scope of the recommendation). Therefore, it's possible to group together
        // most of the aggregate tier demand.
        final Map<CloudTierDemandInfo, AggregateDemand> tierDemandMap = demandSegment.aggregateCloudTierDemandSet()
                .asMap()
                .entrySet()
                .stream()
                .filter(scopedDemandEntry -> tierPricingByScope.containsKey(scopedDemandEntry.getKey()))
                .map(scopedDemandEntry -> {
                    final ScopedCloudTierInfo cloudTierInfo = scopedDemandEntry.getKey();
                    final CloudTierPricingData pricingData = tierPricingByScope.getOrDefault(
                            cloudTierInfo, CloudTierPricingData.EMPTY_PRICING_DATA);
                    final Collection<AggregateCloudTierDemand> aggregateDemandSet =
                            scopedDemandEntry.getValue();

                    final double normalizationFactor = calculateDemandNormalizationFactor(cloudTierInfo);
                    final double onDemandRate = pricingResolver.calculateOnDemandRate(pricingData);
                    final CloudTierDemandInfo demandInfo = CloudTierDemandInfo.builder()
                            .cloudTierDemand(cloudTierInfo.cloudTierDemand())
                            .tierPricingData(pricingData)
                            .normalizedOnDemandRate(onDemandRate / normalizationFactor)
                            .demandCommitmentRatio(calculateDemandCommitmentRatio(cloudTierInfo))
                            .build();

                    final MutableDouble totalDemand = new MutableDouble(0);
                    final MutableDouble coveredDemand = new MutableDouble(0);
                    aggregateDemandSet.forEach(aggregateTierDemand -> {
                        totalDemand.add(aggregateTierDemand.demandAmount());
                        coveredDemand.add(aggregateTierDemand.coverageAmount());
                    });
                    final AggregateDemand aggregateDemand = AggregateDemand.builder()
                            .totalDemand(totalDemand.getValue() * normalizationFactor)
                            .inventoryCoverage(coveredDemand.getValue() * normalizationFactor)
                            .build();

                    return Pair.of(demandInfo, aggregateDemand);
                }).collect(ImmutableMap.toImmutableMap(
                        Pair::getKey,
                        Pair::getValue,
                        // There may be cloud tier demand in separate scopes that resolves to the same
                        // CloudTierDemandInfo instance. Therefore, combine them - the savings calculator
                        // doesn't neeed to know the scope distinctions.
                        AggregateDemand::combine));

        return DemandSegment.builder()
                .timeInterval(demandSegment.timeInterval())
                .putAllTierDemandMap(tierDemandMap)
                .build();
    }

    private RecommendationInfo createRecommendationInfo() {

        final CloudCommitmentType recommendationType = commitmentContext.cloudCommitmentSpecData().type();
        if (recommendationType == CloudCommitmentType.RESERVED_INSTANCE) {

            final Map<Long, Double> demandByAccount = recommendationTopology.demandSegments()
                    .stream()
                    .flatMap(demandSegment -> demandSegment.aggregateCloudTierDemandSet().values().stream())
                    .collect(Collectors.groupingBy(
                            (aggregateDemand) -> aggregateDemand.cloudTierInfo().accountOid(),
                            Collectors.mapping(AggregateCloudTierDemand::demandAmount,
                                    Collectors.reducing(0.0, Double::sum))));

            final long purchasingAccountOid = Collections.max(
                    demandByAccount.entrySet(),
                    Comparator.comparing(Map.Entry::getValue)).getKey();

            return ReservedInstanceRecommendationInfo.builder()
                    .commitmentSpecData((ReservedInstanceSpecData)commitmentContext.cloudCommitmentSpecData())
                    .cloudCommitmentPricingData((RIPricingData)commitmentContext.cloudCommitmentPricingData())
                    .purchasingAccountOid(purchasingAccountOid)
                    .build();

        } else {
            throw new UnsupportedOperationException(
                    String.format("%s is not a supported commitment type", recommendationType));
        }
    }

    @Nonnull
    private CoveredDemandInfo createCoveredDemandInfo() {
        return CoveredDemandInfo.builder()
                .addAllCloudTierSet(commitmentContext.cloudTierPricingByScope().keySet())
                .build();
    }

    private double calculateDemandNormalizationFactor(@Nonnull ScopedCloudTierInfo cloudTierInfo) {

        // Right now, assume compute tier/RI normalization
        final long demandCoupons = computeTierFamilyResolver
                .getNumCoupons(cloudTierInfo.cloudTierDemand().cloudTierOid())
                .orElse(1L);

        return (double)demandCoupons;
    }

    private double calculateDemandCommitmentRatio(@Nonnull ScopedCloudTierInfo cloudTierInfo) {

        // Assumes compute/RI demand, in which the demand:commitment ratio is the same
        // for all cloud tiers.
        final long recommendationTier = ((ReservedInstanceSpecData)commitmentContext.cloudCommitmentSpecData())
                .tierOid();
        final long recommendationCoupons = computeTierFamilyResolver
                .getNumCoupons(recommendationTier)
                .orElse(1L);

        return recommendationCoupons;
    }

    /**
     * A factory class for constructing {@link RecommendationAnalysisTask} instances.
     */
    public static class RecommendationAnalysisTaskFactory {

        private final IdentityProvider identityProvider;

        private final CloudCommitmentSavingsCalculatorFactory savingsCalculatorFactory;

        private final SavingsPricingResolverFactory pricingResolverFactory;

        /**
         * Constructs a new factory instance.
         * @param identityProvider An {@link IdentityProvider} instance, used to assign unique IDs to
         *                         recommendations.
         * @param savingsCalculatorFactory A factory for creating {@link CloudCommitmentSavingsCalculator}
         *                                 instances.
         * @param pricingResolverFactory A factory for creating {@link SavingsPricingResolver} instances.
         */
        public RecommendationAnalysisTaskFactory(
                @Nonnull IdentityProvider identityProvider,
                @Nonnull CloudCommitmentSavingsCalculatorFactory savingsCalculatorFactory,
                @Nonnull SavingsPricingResolverFactory pricingResolverFactory) {

            this.identityProvider = Objects.requireNonNull(identityProvider);
            this.savingsCalculatorFactory = Objects.requireNonNull(savingsCalculatorFactory);
            this.pricingResolverFactory = Objects.requireNonNull(pricingResolverFactory);
        }

        /**
         * Constructs a new {@link RecommendationAnalysisTask} instance.
         * @param recommendationTopology The recommendation topology. All demand contained within the
         *                               topology should be within scope of the recommendation.
         * @param computeTierFamilyResolver The compute tier family resolver, used for normalizing
         *                                  demand to the cloud commitment.
         * @param purchaseProfile The commitment purchase profile, containing the type of cloud
         *                        commitment to recommend, along with purchase settings.
         * @return The newly constructed task.
         */
        @Nonnull
        public RecommendationAnalysisTask newTask(@Nonnull RecommendationTopology recommendationTopology,
                                                  @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                                  @Nonnull CommitmentPurchaseProfile purchaseProfile) {

            Preconditions.checkNotNull(recommendationTopology);
            Preconditions.checkNotNull(computeTierFamilyResolver);
            Preconditions.checkNotNull(purchaseProfile);

            final CloudCommitmentType commitmentType =
                    recommendationTopology.commitmentContext().cloudCommitmentSpecData().type();
            final CloudCommitmentSavingsCalculator savingsCalculator = savingsCalculatorFactory.newCalculator(
                    commitmentType,
                    purchaseProfile.getRecommendationSettings());
            final SavingsPricingResolver pricingResolver = pricingResolverFactory.newResolver(
                    commitmentType,
                    purchaseProfile.getRecommendationSettings());

            return new RecommendationAnalysisTask(
                    identityProvider,
                    savingsCalculator,
                    pricingResolver,
                    recommendationTopology,
                    computeTierFamilyResolver);
        }
    }
}
