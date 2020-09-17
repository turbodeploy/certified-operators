package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ImmutableClassifiedEntityDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStage.DemandTransformationFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

public class DemandTransformationStageTest {

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final AllocatedTransformationPipelineFactory allocatedTransformationPipelineFactory =
            mock(AllocatedTransformationPipelineFactory.class);

    private final DemandTransformationFactory demandTransformationFactory = new DemandTransformationFactory(
            allocatedTransformationPipelineFactory);

    private final TimeInterval analysisWindow = ImmutableTimeInterval.builder()
            .startTime(Instant.ofEpochSecond(Duration.ofDays(1).getSeconds()))
            .endTime(Instant.ofEpochSecond(Duration.ofDays(2).getSeconds()))
            .build();

    private final BoundedDuration analysisBucket = BoundedDuration.builder()
            .amount(1)
            .unit(ChronoUnit.HOURS)
            .build();

    @Test(expected = IllegalStateException.class)
    public void testAnalysisWindowNotSet() throws Exception {

        when(analysisContext.getAnalysisWindow()).thenReturn(Optional.empty());

        final DemandTransformationPipeline demandTransformationPipeline = mock(DemandTransformationPipeline.class);
        when(allocatedTransformationPipelineFactory.newPipeline(any(), any(), any()))
                .thenReturn(demandTransformationPipeline);


        final ClassifiedEntityDemandSet entityDemandSet = ImmutableClassifiedEntityDemandSet.builder().build();
        final AnalysisStage transformationStage = demandTransformationFactory.createStage(
                0, TestUtils.createBaseConfig(), analysisContext);

        transformationStage.execute(entityDemandSet);
    }

    @Test
    public void testAggregation() throws Exception {

        // setup mocks
        when(analysisContext.getAnalysisWindow()).thenReturn(Optional.of(analysisWindow));
        when(analysisContext.getAnalysisBucket()).thenReturn(analysisBucket);

        final DemandTransformationPipeline demandTransformationPipeline = mock(DemandTransformationPipeline.class);
        when(allocatedTransformationPipelineFactory.newPipeline(any(), any(), any()))
                .thenReturn(demandTransformationPipeline);

        // invoke the stage
        final ClassifiedEntityDemandSet entityDemandSet = ImmutableClassifiedEntityDemandSet.builder()
                .addClassifiedAllocatedDemand(ClassifiedEntityDemandAggregate.builder()
                        .entityOid(1)
                        .accountOid(2)
                        .regionOid(3)
                        .serviceProviderOid(4)
                        .build())
                .build();
        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();
        AnalysisStage demandTransformationStage = demandTransformationFactory.createStage(
                123L,
                analysisConfig,
                analysisContext);
        demandTransformationStage.execute(entityDemandSet);

        // verify the allocated demand transformation pipeline creation
        final ArgumentCaptor<CloudCommitmentAnalysisConfig> analysisConfigCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisConfig.class);
        final ArgumentCaptor<CloudCommitmentAnalysisContext> analysisContextCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisContext.class);
        final ArgumentCaptor<DemandTransformationJournal> transformationJournalCaptor =
                ArgumentCaptor.forClass(DemandTransformationJournal.class);
        verify(allocatedTransformationPipelineFactory).newPipeline(
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture(),
                transformationJournalCaptor.capture());

        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo(analysisContext));
        assertThat(transformationJournalCaptor.getValue(), notNullValue());

        // verify the allocation demand is passed into the demand transformation pipeline
        final ArgumentCaptor<Set> allocatedDemandCaptor = ArgumentCaptor.forClass(Set.class);
        verify(demandTransformationPipeline).transformAndCollectDemand(allocatedDemandCaptor.capture());
        assertThat(allocatedDemandCaptor.getValue(), equalTo(entityDemandSet.classifiedAllocatedDemand()));


    }
}
