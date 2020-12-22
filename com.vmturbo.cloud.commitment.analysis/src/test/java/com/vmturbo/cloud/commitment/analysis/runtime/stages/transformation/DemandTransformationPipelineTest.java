package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationPipeline.DemandTransformationPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassifiedEntitySelection;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class DemandTransformationPipelineTest {

    private final AnalysisSelector analysisSelector = mock(AnalysisSelector.class);
    private final DemandTransformer demandTransformer = mock(DemandTransformer.class);
    private final RecommendationSelector recommendationSelector = mock(RecommendationSelector.class);
    private final AggregateDemandCollector aggregateDemandCollector = mock(AggregateDemandCollector.class);

    private final DemandTransformationPipelineFactory pipelineFactory = new DemandTransformationPipelineFactory();


    private final DemandClassification allocatedClassification = DemandClassification.of(
            AllocatedDemandClassification.ALLOCATED);
    private final DemandTimeSeries allocatedSeries = DemandTimeSeries.builder()
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(5)
                    .osType(OSType.RHEL)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .build();
    private final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
            .entityOid(1)
            .accountOid(2)
            .regionOid(3)
            .serviceProviderOid(4)
            .putClassifiedCloudTierDemand(
                    allocatedClassification,
                    Collections.singleton(allocatedSeries))
            .build();

    private final ClassifiedEntitySelection entitySelection = ClassifiedEntitySelection.builder()
            .entityOid(entityAggregate.entityOid())
            .accountOid(entityAggregate.accountOid())
            .regionOid(entityAggregate.regionOid())
            .serviceProviderOid(entityAggregate.serviceProviderOid())
            .classification(allocatedClassification)
            .demandTimeline(allocatedSeries.demandIntervals())
            .cloudTierDemand(allocatedSeries.cloudTierDemand())
            .isSuspended(false)
            .isTerminated(false)
            .isRecommendationCandidate(false)
            .build();

    @Test
    public void testPipeline() {

        when(analysisSelector.filterEntityAggregate(any())).thenReturn(entityAggregate);
        when(demandTransformer.transformDemand(any())).thenReturn(entityAggregate);
        when(recommendationSelector.selectRecommendationDemand(any()))
                .thenReturn(Collections.singleton(entitySelection));

        final DemandTransformationPipeline pipeline = pipelineFactory.newPipeline(
                analysisSelector,
                demandTransformer,
                recommendationSelector,
                aggregateDemandCollector);
        pipeline.transformAndCollectDemand(Collections.singleton(entityAggregate));

        final ArgumentCaptor<ClassifiedEntityDemandAggregate> aggregateCaptor =
                ArgumentCaptor.forClass(ClassifiedEntityDemandAggregate.class);

        // verify analysis selection invocation
        verify(analysisSelector).filterEntityAggregate(aggregateCaptor.capture());
        assertThat(aggregateCaptor.getValue(), equalTo(entityAggregate));

        // verify demand transformer invocation
        verify(demandTransformer).transformDemand(aggregateCaptor.capture());
        assertThat(aggregateCaptor.getValue(), equalTo(entityAggregate));

        // verify recommendation selector invocation
        verify(recommendationSelector).selectRecommendationDemand(aggregateCaptor.capture());
        assertThat(aggregateCaptor.getValue(), equalTo(entityAggregate));

        // verify aggregate collector invocation
        final ArgumentCaptor<ClassifiedEntitySelection> entitySelectionCaptor =
                ArgumentCaptor.forClass(ClassifiedEntitySelection.class);
        verify(aggregateDemandCollector).collectEntitySelection(entitySelectionCaptor.capture());
        assertThat(entitySelectionCaptor.getValue(), equalTo(entitySelection));
    }
}
