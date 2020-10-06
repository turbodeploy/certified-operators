package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandCollector.AggregateDemandCollectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationPipeline.DemandTransformationPipelineFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.FlexibleRIComputeTransformer.FlexibleRIComputeTransformerFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector.AnalysisSelectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector.RecommendationSelectorFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.cost.calculation.integration.CloudTopology;

public class AllocatedTransformationPipelineFactoryTest {

    private final DemandTransformationPipelineFactory demandTransformationPipelineFactory =
            mock(DemandTransformationPipelineFactory.class);

    private final AnalysisSelectorFactory analysisSelectorFactory = mock(AnalysisSelectorFactory.class);

    private final FlexibleRIComputeTransformerFactory flexibleRIComputeTransformerFactory =
            mock(FlexibleRIComputeTransformerFactory.class);

    private final RecommendationSelectorFactory recommendationSelectorFactory =
            mock(RecommendationSelectorFactory.class);

    private final AggregateDemandCollectorFactory aggregateDemandCollectorFactory =
            mock(AggregateDemandCollectorFactory.class);

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final DemandTransformationJournal transformationJournal = mock(DemandTransformationJournal.class);

    private AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory;

    @Before
    public void setup() {
        allocatedTransformationPipelineFactory = new AllocatedTransformationPipelineFactory(
                demandTransformationPipelineFactory,
                analysisSelectorFactory,
                flexibleRIComputeTransformerFactory,
                recommendationSelectorFactory,
                aggregateDemandCollectorFactory);
    }

    @Test(expected = IllegalStateException.class)
    public void testAnalysisWindowNotSet() {

        // setup the analysis context
        when(analysisContext.getRecommendationType()).thenReturn(CloudCommitmentType.RESERVED_INSTANCE);
        when(analysisContext.getAnalysisWindow()).thenReturn(Optional.empty());

        final DemandTransformationPipeline transformationPipeline =
                allocatedTransformationPipelineFactory.newPipeline(
                        TestUtils.createBaseConfig(),
                        analysisContext,
                        transformationJournal);
    }


    @Test
    public void testSuccessfulPipelineCreation() {
        when(analysisContext.getRecommendationType()).thenReturn(CloudCommitmentType.RESERVED_INSTANCE);
        final TimeInterval analysisWindow = ImmutableTimeInterval.builder()
                .startTime(Instant.ofEpochSecond(1))
                .endTime(Instant.ofEpochSecond(3))
                .build();
        when(analysisContext.getAnalysisWindow()).thenReturn(Optional.of(analysisWindow));
        final BoundedDuration analysisBucket = BoundedDuration.builder()
                .amount(123)
                .unit(ChronoUnit.MINUTES)
                .build();
        when(analysisContext.getAnalysisBucket()).thenReturn(analysisBucket);
        final ComputeTierFamilyResolver familyResolver = mock(ComputeTierFamilyResolver.class);
        when(analysisContext.getComputeTierFamilyResolver()).thenReturn(familyResolver);

        // setup the factory mocks
        final AnalysisSelector analysisSelector = mock(AnalysisSelector.class);
        when(analysisSelectorFactory.newSelector(any(), any())).thenReturn(analysisSelector);

        final FlexibleRIComputeTransformer flexibleRIComputeTransformer =
                mock(FlexibleRIComputeTransformer.class);
        when(flexibleRIComputeTransformerFactory.newTransformer(any(), any()))
                .thenReturn(flexibleRIComputeTransformer);

        final RecommendationSelector recommendationSelector = mock(RecommendationSelector.class);
        when(recommendationSelectorFactory.fromDemandSelection(any())).thenReturn(recommendationSelector);

        final AggregateDemandCollector aggregateDemandCollector = mock(AggregateDemandCollector.class);
        when(aggregateDemandCollectorFactory.newCollector(any(), any(), any(), any()))
                .thenReturn(aggregateDemandCollector);

        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();
        final DemandTransformationPipeline transformationPipeline =
                allocatedTransformationPipelineFactory.newPipeline(
                        analysisConfig,
                        analysisContext,
                        transformationJournal);

        // verify the analysis selector creation
        final ArgumentCaptor<DemandTransformationJournal> journalCaptor =
                ArgumentCaptor.forClass(DemandTransformationJournal.class);
        final ArgumentCaptor<CloudTopology> cloudTopologyCaptor = ArgumentCaptor.forClass(CloudTopology.class);
        final ArgumentCaptor<AllocatedDemandSelection> demandSelectionCaptor =
                ArgumentCaptor.forClass(AllocatedDemandSelection.class);
        verify(analysisSelectorFactory).newSelector(
                journalCaptor.capture(),
                demandSelectionCaptor.capture());
        assertThat(journalCaptor.getValue(), equalTo(transformationJournal));
        assertThat(demandSelectionCaptor.getValue(), equalTo(analysisConfig.getDemandSelection().getAllocatedSelection()));

        // verify flexibleRIComputeTransformerFactory invocation
        final ArgumentCaptor<ComputeTierFamilyResolver> familyResolverCaptor =
                ArgumentCaptor.forClass(ComputeTierFamilyResolver.class);
        verify(flexibleRIComputeTransformerFactory).newTransformer(
                journalCaptor.capture(),
                familyResolverCaptor.capture());
        assertThat(journalCaptor.getValue(), equalTo(transformationJournal));
        assertThat(familyResolverCaptor.getValue(), equalTo(familyResolver));

        // verify the recommendation selector creation
        verify(recommendationSelectorFactory).fromDemandSelection(demandSelectionCaptor.capture());
        assertThat(demandSelectionCaptor.getValue(), equalTo(analysisConfig.getPurchaseProfile().getAllocatedSelection()));

        // verify the aggregate demand collector creation
        final ArgumentCaptor<TimeInterval> timeIntervalCaptor = ArgumentCaptor.forClass(TimeInterval.class);
        final ArgumentCaptor<BoundedDuration> boundedDurationCaptor =
                ArgumentCaptor.forClass(BoundedDuration.class);
        verify(aggregateDemandCollectorFactory).newCollector(
                journalCaptor.capture(),
                cloudTopologyCaptor.capture(),
                timeIntervalCaptor.capture(),
                boundedDurationCaptor.capture());
        assertThat(journalCaptor.getValue(), equalTo(transformationJournal));
        assertThat(timeIntervalCaptor.getValue(), equalTo(analysisWindow));
        assertThat(boundedDurationCaptor.getValue(), equalTo(analysisBucket));

        // verify the transformation pipeline creation
        final ArgumentCaptor<AnalysisSelector> analysisSelectorCaptor =
                ArgumentCaptor.forClass(AnalysisSelector.class);
        final ArgumentCaptor<DemandTransformer> transformerCaptor =
                ArgumentCaptor.forClass(DemandTransformer.class);
        final ArgumentCaptor<RecommendationSelector> recommendationSelectorCaptor =
                ArgumentCaptor.forClass(RecommendationSelector.class);
        final ArgumentCaptor<AggregateDemandCollector> collectorCaptor =
                ArgumentCaptor.forClass(AggregateDemandCollector.class);
        verify(demandTransformationPipelineFactory).newPipeline(
                analysisSelectorCaptor.capture(),
                transformerCaptor.capture(),
                recommendationSelectorCaptor.capture(),
                collectorCaptor.capture());
        assertThat(analysisSelectorCaptor.getValue(), equalTo(analysisSelector));
        assertThat(transformerCaptor.getValue(), equalTo(flexibleRIComputeTransformer));
        assertThat(recommendationSelectorCaptor.getValue(), equalTo(recommendationSelector));
        assertThat(collectorCaptor.getValue(), equalTo(aggregateDemandCollector));
    }

}
