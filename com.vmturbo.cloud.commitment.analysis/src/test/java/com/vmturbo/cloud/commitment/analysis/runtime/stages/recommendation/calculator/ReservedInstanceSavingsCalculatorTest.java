package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.ReservedInstanceSavingsCalculator.ReservedInstanceSavingsCalculatorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.AggregateDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.CloudTierDemandInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.DemandSegment;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings.ReservedInstanceRecommendationSettings;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class ReservedInstanceSavingsCalculatorTest {

    private static final double TOLERANCE = .001;

    private final ReservedInstanceSavingsCalculatorFactory calculatorFactory =
            new ReservedInstanceSavingsCalculatorFactory();

    private final RecommendationSettings defaultSettings = RecommendationSettings.newBuilder()
            .setReservedInstanceSettings(ReservedInstanceRecommendationSettings.newBuilder())
            .setMinimumSavingsOverOnDemandPercent(10.0)
            .setMaxDemandPercent(100.0)
            .build();

    private final ComputeTierDemand demandA = ComputeTierDemand.builder()
            .cloudTierOid(1)
            .osType(OSType.LINUX)
            .tenancy(Tenancy.DEFAULT)
            .build();

    private final TimeInterval timeInterval = TimeInterval.builder()
            .startTime(Instant.ofEpochSecond(0))
            .endTime(Instant.ofEpochSecond(0).plus(1, ChronoUnit.HOURS))
            .build();


    /**
     * Test a simple example where saving over on-demand is only 9% (below 10% threshold).
     */
    @Test
    public void testSavingsBelowTarget() {

        // setup the demand
        final CloudTierDemandInfo demandInfo = CloudTierDemandInfo.builder()
                .cloudTierDemand(demandA)
                .tierPricingData(CloudTierPricingData.EMPTY_PRICING_DATA)
                .normalizedOnDemandRate(1.0)
                .demandCommitmentRatio(1.0)
                .build();
        final AggregateDemand aggregateDemand = AggregateDemand.builder()
                .totalDemand(1.0)
                .inventoryCoverage(0)
                .build();
        final DemandSegment demandSegment = DemandSegment.builder()
                .timeInterval(timeInterval)
                .putTierDemandMap(demandInfo, aggregateDemand)
                .build();
        final SavingsCalculationContext savingsContext = SavingsCalculationContext.builder()
                // just below 10% discount
                .amortizedCommitmentRate(.91)
                .addDemandSegment(demandSegment)
                .build();

        // Invoke the savings calculator
        final CloudCommitmentSavingsCalculator calculator = calculatorFactory.newCalculator(defaultSettings);
        final SavingsCalculationResult savingsResult = calculator.calculateSavingsRecommendation(savingsContext);

        assertThat(savingsResult.recommendation().recommendationQuantity(), equalTo(0L));
    }

    /**
     * Tests a recommendation with a single demand segment in which the recommendation savings over
     * on-demand is at the target percentage and some demand is left uncovered.
     */
    @Test
    public void testSavingsAtTarget() {

        // setup the demand
        final CloudTierDemandInfo demandInfo = CloudTierDemandInfo.builder()
                .cloudTierDemand(demandA)
                .tierPricingData(CloudTierPricingData.EMPTY_PRICING_DATA)
                .normalizedOnDemandRate(2.0)
                .demandCommitmentRatio(1.0)
                .build();
        final AggregateDemand aggregateDemand = AggregateDemand.builder()
                .totalDemand(2.0)
                .inventoryCoverage(.5)
                .build();
        final DemandSegment demandSegment = DemandSegment.builder()
                .timeInterval(timeInterval)
                .putTierDemandMap(demandInfo, aggregateDemand)
                .build();
        final SavingsCalculationContext savingsContext = SavingsCalculationContext.builder()
                // just below 10% discount
                .amortizedCommitmentRate(1.80)
                .addDemandSegment(demandSegment)
                .build();

        // Invoke the savings calculator
        final CloudCommitmentSavingsCalculator calculator = calculatorFactory.newCalculator(defaultSettings);
        final SavingsCalculationResult savingsResult = calculator.calculateSavingsRecommendation(savingsContext);

        // Assertions
        final SavingsCalculationRecommendation recommendation = savingsResult.recommendation();
        assertThat(recommendation.recommendationQuantity(), equalTo(1L));
        assertThat(recommendation.aggregateNormalizedDemand(), closeTo(2.0, TOLERANCE));
        assertThat(recommendation.aggregateOnDemandCost(), closeTo(3.0, TOLERANCE));
        assertThat(recommendation.appraisalDuration(), equalTo(Duration.ofHours(1)));
        assertThat(recommendation.coveredDemand(), closeTo(1.0, TOLERANCE));
        assertThat(recommendation.coveredOnDemandCost(), closeTo(2.0, TOLERANCE));
        assertThat(recommendation.initialUncoveredDemand(), closeTo(1.5, TOLERANCE));
        assertThat(recommendation.inventoryCoverage(), closeTo(25.0, TOLERANCE));
        assertThat(recommendation.recommendationCost(), closeTo(1.80, TOLERANCE));
        assertThat(recommendation.recommendationCoverage(), closeTo(66.666, TOLERANCE));
        assertThat(recommendation.recommendationUtilization(), closeTo(100.0, TOLERANCE));
        assertThat(recommendation.savings(), closeTo(.2, TOLERANCE));
        assertThat(recommendation.savingsOverOnDemand(), closeTo(10.0, TOLERANCE));
    }

    /**
     * Tests a max covered setting, comparing it to results with 100% coverage.
     */
    @Test
    public void testDemandReservation() {

        // setup the demand
        final CloudTierDemandInfo demandInfo = CloudTierDemandInfo.builder()
                .cloudTierDemand(demandA)
                .tierPricingData(CloudTierPricingData.EMPTY_PRICING_DATA)
                .normalizedOnDemandRate(2.0)
                .demandCommitmentRatio(1.0)
                .build();
        final AggregateDemand aggregateDemand = AggregateDemand.builder()
                .totalDemand(2.0)
                .inventoryCoverage(0)
                .build();
        final DemandSegment demandSegment = DemandSegment.builder()
                .timeInterval(timeInterval)
                .putTierDemandMap(demandInfo, aggregateDemand)
                .build();
        final SavingsCalculationContext savingsContext = SavingsCalculationContext.builder()
                // just below 10% discount
                .amortizedCommitmentRate(1.80)
                .addDemandSegment(demandSegment)
                .build();


        // Check that 2 recommendations are made with default (no reservations)
        final CloudCommitmentSavingsCalculator baseCalculator = calculatorFactory.newCalculator(defaultSettings);
        final SavingsCalculationResult baseResults = baseCalculator.calculateSavingsRecommendation(savingsContext);

        // Setup reserved demand
        final RecommendationSettings reservedSettings = RecommendationSettings.newBuilder()
                .setReservedInstanceSettings(ReservedInstanceRecommendationSettings.newBuilder())
                .setMinimumSavingsOverOnDemandPercent(10.0)
                .setMaxDemandPercent(90.0)
                .build();
        final CloudCommitmentSavingsCalculator reservedCalculator = calculatorFactory.newCalculator(reservedSettings);
        final SavingsCalculationResult reservedResults = reservedCalculator.calculateSavingsRecommendation(savingsContext);

        // Assertions
        assertThat(baseResults.recommendation().recommendationQuantity(), equalTo(2L));
        assertThat(reservedResults.recommendation().recommendationQuantity(), equalTo(1L));
    }
}
