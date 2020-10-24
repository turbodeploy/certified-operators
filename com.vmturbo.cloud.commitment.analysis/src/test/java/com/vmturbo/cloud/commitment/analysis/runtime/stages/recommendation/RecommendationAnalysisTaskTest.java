package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisTask.RecommendationAnalysisTaskFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationTopology.RecommendationDemandSegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.SavingsPricingResolver.SavingsPricingResolverFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.CloudCommitmentSavingsCalculatorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ImmutableReservedInstanceSpecData;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class RecommendationAnalysisTaskTest {

    private final CloudCommitmentSavingsCalculatorFactory savingsCalculatorFactory =
            mock(CloudCommitmentSavingsCalculatorFactory.class);

    private final CloudCommitmentSavingsCalculator savingsCalculator = mock(CloudCommitmentSavingsCalculator.class);

    private final SavingsCalculationResult savingsCalculationResult = mock(SavingsCalculationResult.class);

    private final SavingsCalculationRecommendation savingsRecommendation = mock(SavingsCalculationRecommendation.class);

    private final SavingsPricingResolverFactory pricingResolverFactory = mock(SavingsPricingResolverFactory.class);

    private final SavingsPricingResolver pricingResolver = mock(SavingsPricingResolver.class);

    private final RecommendationAnalysisTaskFactory taskFactory = new RecommendationAnalysisTaskFactory(
            new DefaultIdentityProvider(0),
            savingsCalculatorFactory,
            pricingResolverFactory);

    private final ComputeTierFamilyResolver computeTierFamilyResolver = mock(ComputeTierFamilyResolver.class);

    // Setup the recommendation topology
    private final ScopedCloudTierInfo cloudTierInfoA = ScopedCloudTierInfo.builder()
            .accountOid(1)
            .regionOid(2)
            .serviceProviderOid(3)
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(4)
                    .osType(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .build();

    private final CloudTierPricingData pricingDataA = mock(CloudTierPricingData.class);

    private final ScopedCloudTierInfo cloudTierInfoB = ScopedCloudTierInfo.builder()
            .from(cloudTierInfoA)
            .accountOid(5)
            .build();

    private final CloudTierPricingData pricingDataB = mock(CloudTierPricingData.class);

    private final CloudCommitmentPricingData commitmentPricingData = RIPricingData.builder()
            .hourlyRecurringRate(1.0)
            .hourlyUpFrontRate(2.0)
            .build();

    private final CloudCommitmentSpecData commitmentSpecData = ImmutableReservedInstanceSpecData.builder()
            .spec(ReservedInstanceSpec.newBuilder().build())
            .cloudTier(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .setOid(123L)
                    .build())
            .build();

    private final RateAnnotatedCommitmentContext commitmentContext = RateAnnotatedCommitmentContext.builder()
            .putCloudTierPricingByScope(cloudTierInfoA, pricingDataA)
            .putCloudTierPricingByScope(cloudTierInfoB, pricingDataB)
            .cloudCommitmentPricingData(commitmentPricingData)
            .cloudCommitmentSpecData(commitmentSpecData)
            .build();

    private final AggregateCloudTierDemand aggregateDemandA = AggregateCloudTierDemand.builder()
            .cloudTierInfo(cloudTierInfoA)
            .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
            .isRecommendationCandidate(true)
            .putDemandByEntity(EntityInfo.builder().entityOid(8).build(), 10.0)
            .build();

    private final AggregateCloudTierDemand aggregateDemandB = AggregateCloudTierDemand.builder()
            .cloudTierInfo(cloudTierInfoB)
            .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
            .isRecommendationCandidate(true)
            .putDemandByEntity(EntityInfo.builder().entityOid(8).build(), 20.0)
            .build();

    private final RecommendationTopology recommendationTopology = RecommendationTopology.builder()
            .commitmentContext(commitmentContext)
            .addDemandSegment(RecommendationDemandSegment.builder()
                    .timeInterval(TimeInterval.builder()
                            .startTime(Instant.ofEpochSecond(0))
                            .endTime(Instant.ofEpochSecond(0).plus(1, ChronoUnit.HOURS))
                            .build())
                    .putAggregateCloudTierDemandSet(cloudTierInfoA, aggregateDemandA)
                    .putAggregateCloudTierDemandSet(cloudTierInfoB, aggregateDemandB)
                    .build())
            .build();

    @Before
    public void setup() {
        when(savingsCalculatorFactory.newCalculator(any(), any())).thenReturn(savingsCalculator);
        when(savingsCalculator.calculateSavingsRecommendation(any())).thenReturn(savingsCalculationResult);
        when(savingsCalculationResult.recommendation()).thenReturn(savingsRecommendation);
        when(pricingResolverFactory.newResolver(any(), any())).thenReturn(pricingResolver);

        when(computeTierFamilyResolver.getNumCoupons(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    public void testRIRecommendationInfo() throws Exception {

        final RecommendationAnalysisTask analysisTask = taskFactory.newTask(
                recommendationTopology,
                computeTierFamilyResolver,
                CommitmentPurchaseProfile.newBuilder().build());

        final CloudCommitmentRecommendation recommendation = analysisTask.call();

        // ASSERTIONS
        final ReservedInstanceRecommendationInfo recommendationInfo = (ReservedInstanceRecommendationInfo)
                recommendation.recommendationInfo();
        // demand B has the most demand - it should be the purchasing acct
        assertThat(recommendationInfo.purchasingAccountOid(), equalTo(cloudTierInfoB.accountOid()));
    }
}
