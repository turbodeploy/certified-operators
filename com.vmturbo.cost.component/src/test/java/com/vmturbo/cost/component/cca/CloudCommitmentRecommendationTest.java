package com.vmturbo.cost.component.cca;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.ComputeTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CoveredDemandInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.ReservedInstanceRecommendationInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.AggregateDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.CloudTierDemandInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext.DemandSegment;
import com.vmturbo.cloud.commitment.analysis.spec.ImmutableReservedInstanceSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * A utility class for holding test definitions of recommendations.
 */
public class CloudCommitmentRecommendationTest {

    private CloudCommitmentRecommendationTest() {}

    /**
     * RI pricing data for a recommendation.
     */
    public static final RIPricingData RI_PRICING_DATA = RIPricingData.builder()
            .hourlyUpFrontRate(2.0)
            .hourlyRecurringRate(3.0)
            .build();

    /**
     * RI spec info.
     */
    public static final ReservedInstanceSpecInfo RESERVED_INSTANCE_SPEC_INFO = ReservedInstanceSpecInfo.newBuilder()
            .setTierId(1L)
            .setRegionId(2L)
            .setOs(OSType.RHEL)
            .setPlatformFlexible(true)
            .setSizeFlexible(true)
            .setType(ReservedInstanceType.newBuilder().setTermYears(1).build())
            .build();

    /**
     * RI spec data.
     */
    public static final ReservedInstanceSpecData RESERVED_INSTANCE_SPEC_DATA = ImmutableReservedInstanceSpecData.builder()
            .spec(ReservedInstanceSpec.newBuilder()
                    .setId(3L)
                    .setReservedInstanceSpecInfo(RESERVED_INSTANCE_SPEC_INFO)
                    .build())
            .cloudTier(TopologyEntityDTO.newBuilder()
                    .setOid(4L)
                    .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .build())
            .build();

    /**
     * Demand for cloud tier "A".
     */
    public static final CloudTierDemand CLOUD_TIER_A = ComputeTierDemand.builder()
            .cloudTierOid(1L)
            .osType(OSType.RHEL)
            .tenancy(Tenancy.DEFAULT)
            .build();

    /**
     * Demand for cloud tier "B".
     */
    public static final CloudTierDemand CLOUD_TIER_B = ComputeTierDemand.builder()
            .cloudTierOid(1L)
            .osType(OSType.WINDOWS)
            .tenancy(Tenancy.DEFAULT)
            .build();

    /**
     * Pricing data for cloud tier A.
     */
    public static final CloudTierPricingData PRICING_DATA_A = ComputeTierPricingData.builder()
            .onDemandComputeRate(5.0)
            .onDemandLicenseRate(2.5)
            .build();

    /**
     * Pricing data for cloud tier B.
     */
    public static final CloudTierPricingData PRICING_DATA_B = ComputeTierPricingData.builder()
            .onDemandComputeRate(5.0)
            .onDemandLicenseRate(5.0)
            .build();

    /**
     * Demand info for cloud tier A.
     */
    public static final CloudTierDemandInfo CLOUD_TIER_DEMAND_INFO_A = CloudTierDemandInfo.builder()
            .tierPricingData(PRICING_DATA_A)
            .cloudTierDemand(CLOUD_TIER_A)
            .demandCommitmentRatio(1.0)
            .normalizedOnDemandRate(7.5)
            .build();

    /**
     * Demand info for cloud tier B.
     */
    public static final CloudTierDemandInfo CLOUD_TIER_DEMAND_INFO_B = CloudTierDemandInfo.builder()
            .tierPricingData(PRICING_DATA_B)
            .cloudTierDemand(CLOUD_TIER_B)
            .demandCommitmentRatio(1.0)
            .normalizedOnDemandRate(10.0)
            .build();

    /**
     * Savings calculation demand segment 1.
     */
    public static final DemandSegment DEMAND_SEGMENT_1 = DemandSegment.builder()
            .timeInterval(TimeInterval.builder()
                    .startTime(Instant.ofEpochSecond(0))
                    .endTime(Instant.ofEpochSecond(0).plus(1, ChronoUnit.HOURS))
                    .build())
            .putTierDemandMap(CLOUD_TIER_DEMAND_INFO_A, AggregateDemand.builder()
                    .inventoryCoverage(1.0)
                    .totalDemand(2.0)
                    .build())
            .putTierDemandMap(CLOUD_TIER_DEMAND_INFO_B, AggregateDemand.builder()
                    .inventoryCoverage(1.0)
                    .totalDemand(2.0)
                    .build())
            .build();

    /**
     * Savings calculation demand segment 2.
     */
    public static final DemandSegment DEMAND_SEGMENT_2 = DemandSegment.builder()
            .timeInterval(TimeInterval.builder()
                    .startTime(Instant.ofEpochSecond(0).plus(1, ChronoUnit.HOURS))
                    .endTime(Instant.ofEpochSecond(0).plus(2, ChronoUnit.HOURS))
                    .build())
            .putTierDemandMap(CLOUD_TIER_DEMAND_INFO_A, AggregateDemand.builder()
                    .inventoryCoverage(0)
                    .totalDemand(2.0)
                    .build())
            .putTierDemandMap(CLOUD_TIER_DEMAND_INFO_B, AggregateDemand.builder()
                    .inventoryCoverage(1.0)
                    .totalDemand(2.0)
                    .build())
            .build();

    /**
     * A savings calculation recommendation for an RI.
     */
    public static final SavingsCalculationRecommendation RI_SAVINGS_CALCULATION_RECOMMENDATION = SavingsCalculationRecommendation.builder()
            .recommendationCost(10.0)
            .recommendationCoverage(80.0)
            .recommendationQuantity(2)
            .recommendationUtilization(100.0)
            .savings(10.0)
            .savingsOverOnDemand(25.0)
            .aggregateNormalizedDemand(8)
            .aggregateOnDemandCost(42.5)
            .appraisalDuration(Duration.ofHours(2))
            .coveredDemand(4)
            .coveredOnDemandCost(32.5)
            .initialUncoveredDemand(5.0)
            .inventoryCoverage(37.5)
            .build();

    /**
     * A savings calculation context for an RI.
     */
    public static final SavingsCalculationContext RI_SAVINGS_CALCULATION_CONTEXT = SavingsCalculationContext.builder()
            .amortizedCommitmentRate(5.0)
            .demandSegments(TimeSeries.newTimeSeries(
                    DEMAND_SEGMENT_1,
                    DEMAND_SEGMENT_2))
            .build();

    /**
     * Scoped cloud tier for demand "A".
     */
    public static final ScopedCloudTierInfo SCOPED_CLOUD_TIER_INFO_A = ScopedCloudTierInfo.builder()
            .accountOid(5L)
            .regionOid(2L)
            .serviceProviderOid(10L)
            .cloudTierDemand(CLOUD_TIER_A)
            .build();

    /**
     * Scoped cloud tier for demand "B".
     */
    public static final ScopedCloudTierInfo SCOPED_CLOUD_TIER_INFO_B = ScopedCloudTierInfo.builder()
            .accountOid(5L)
            .regionOid(2L)
            .serviceProviderOid(10L)
            .cloudTierDemand(CLOUD_TIER_B)
            .build();

    /**
     * An RI recommendation.
     */
    public static final CloudCommitmentRecommendation RESERVED_INSTANCE_RECOMMENDATION = CloudCommitmentRecommendation.builder()
            .recommendationId(1L)
            .recommendationInfo(ReservedInstanceRecommendationInfo.builder()
                    .commitmentSpecData(RESERVED_INSTANCE_SPEC_DATA)
                    .cloudCommitmentPricingData(RI_PRICING_DATA)
                    .purchasingAccountOid(5L)
                    .build())
            .savingsCalculationResult(SavingsCalculationResult.builder()
                    .recommendationContext(RI_SAVINGS_CALCULATION_CONTEXT)
                    .recommendation(RI_SAVINGS_CALCULATION_RECOMMENDATION)
                    .build())
            .coveredDemandInfo(CoveredDemandInfo.builder()
                    .addCloudTierSet(SCOPED_CLOUD_TIER_INFO_A)
                    .addCloudTierSet(SCOPED_CLOUD_TIER_INFO_B)
                    .build())
            .build();
}

