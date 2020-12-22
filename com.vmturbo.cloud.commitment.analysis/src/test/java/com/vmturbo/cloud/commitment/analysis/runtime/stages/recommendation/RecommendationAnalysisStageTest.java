package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopologySegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.pricing.PricingResolverOutput;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisStage.RecommendationAnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.RecommendationAnalysisTask.RecommendationAnalysisTaskFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.CloudCommitmentSavingsCalculator.SavingsCalculationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator.SavingsCalculationContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class RecommendationAnalysisStageTest {

    // setup the input data
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

    private final ScopedCloudTierInfo cloudTierInfoB = ScopedCloudTierInfo.builder()
            .from(cloudTierInfoA)
            .accountOid(6)
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(5)
                    .osType(OSType.LINUX)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
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

    private final AnalysisTopology analysisTopology = AnalysisTopology.builder()
            .addSegment(AnalysisTopologySegment.builder()
                    .timeInterval(TimeInterval.builder()
                            .startTime(Instant.ofEpochSecond(0))
                            .endTime(Instant.ofEpochSecond(0).plus(1, ChronoUnit.HOURS))
                            .build())
                    .putAggregateCloudTierDemandSet(aggregateDemandA.cloudTierInfo(), aggregateDemandA)
                    .putAggregateCloudTierDemandSet(aggregateDemandB.cloudTierInfo(), aggregateDemandB)
                    .build())
            .build();

    private final RateAnnotatedCommitmentContext commitmentContext = RateAnnotatedCommitmentContext.builder()
            .cloudCommitmentSpecData(mock(CloudCommitmentSpecData.class))
            .cloudCommitmentPricingData(mock(CloudCommitmentPricingData.class))
            .putCloudTierPricingByScope(cloudTierInfoA, CloudTierPricingData.EMPTY_PRICING_DATA)
            .putCloudTierPricingByScope(cloudTierInfoB, CloudTierPricingData.EMPTY_PRICING_DATA)
            .build();

    private final PricingResolverOutput pricingResolverOutput = PricingResolverOutput.builder()
            .addRateAnnotatedCommitmentContextSet(commitmentContext)
            .analysisTopology(analysisTopology)
            .build();

    private final RecommendationAnalysisTaskFactory recommendationTaskFactory =
            mock(RecommendationAnalysisTaskFactory.class);

    private final RecommendationAnalysisTask recommendationAnalysisTask = mock(RecommendationAnalysisTask.class);

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final MinimalCloudTopology cloudTopology = mock(MinimalCloudTopology.class);

    private final RecommendationAnalysisFactory stageFactory = new RecommendationAnalysisFactory(
            recommendationTaskFactory,
            Duration.ofSeconds(10));

    private final SavingsCalculationRecommendation savingsCalculationRecommendation =
            mock(SavingsCalculationRecommendation.class);

    private final CloudCommitmentRecommendation cloudCommitmentRecommendation = CloudCommitmentRecommendation.builder()
            .recommendationId(1)
            .recommendationInfo(mock(ReservedInstanceRecommendationInfo.class))
            .coveredDemandInfo(mock(CoveredDemandInfo.class))
            .savingsCalculationResult(SavingsCalculationResult.builder()
                    .recommendationContext(mock(SavingsCalculationContext.class))
                    .recommendation(savingsCalculationRecommendation)
                    .build())
            .build();


    @Before
    public void setup() throws Exception {
        when(recommendationTaskFactory.newTask(any(), any(), any())).thenReturn(recommendationAnalysisTask);
        when(recommendationAnalysisTask.call()).thenReturn(cloudCommitmentRecommendation);
        when(analysisContext.getAnalysisExecutorService()).thenReturn(Executors.newSingleThreadExecutor());
        when(analysisContext.getSourceCloudTopology()).thenReturn(cloudTopology);
        when(cloudTopology.getEntity(anyLong())).thenReturn(Optional.empty());

        when(savingsCalculationRecommendation.rejectedRecommendation()).thenReturn(Optional.empty());
        when(cloudCommitmentRecommendation.recommendationInfo().commitmentType()).thenReturn(CloudCommitmentType.RESERVED_INSTANCE);
    }

    @Test
    public void testRecommendationTaskCreation() throws Exception {

        final AnalysisStage<PricingResolverOutput, CloudCommitmentRecommendations> analysisStage = stageFactory.createStage(
                123,
                TestUtils.createBaseConfig(),
                analysisContext);

        analysisStage.execute(pricingResolverOutput);

        // There should be two tasks generated as there are two accounts without associated billing
        // families.
        final ArgumentCaptor<RecommendationTopology> recommendationTopologyCaptor =
                ArgumentCaptor.forClass(RecommendationTopology.class);
        verify(recommendationTaskFactory, times(2))
                .newTask(recommendationTopologyCaptor.capture(), any(), any());

        final List<RecommendationTopology> actualTopologies = recommendationTopologyCaptor.getAllValues();
        assertThat(actualTopologies.get(0).commitmentContext(), equalTo(commitmentContext));
        assertThat(actualTopologies.get(1).commitmentContext(), equalTo(commitmentContext));
    }
}
