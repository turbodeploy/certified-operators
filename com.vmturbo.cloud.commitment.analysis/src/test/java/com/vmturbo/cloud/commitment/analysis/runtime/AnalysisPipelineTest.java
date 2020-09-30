package com.vmturbo.cloud.commitment.analysis.runtime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval.DemandRetrievalStage.DemandRetrievalFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval.EntityCloudTierDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateAnalysisDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStage.DemandTransformationFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * Tests {@link AnalysisPipelineFactory} and {@link AnalysisPipeline}.
 */
public class AnalysisPipelineTest {

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final InitializationStageFactory initializationStageFactory = mock(InitializationStageFactory.class);

    private final DemandRetrievalFactory demandRetrievalFactory = mock(DemandRetrievalFactory.class);

    private final DemandClassificationFactory demandClassificationFactory = mock(DemandClassificationFactory.class);

    private final DemandTransformationFactory demandTransformationFactory = mock(DemandTransformationFactory.class);

    private final AnalysisPipelineFactory analysisPipelineFactory =
            new AnalysisPipelineFactory(identityProvider,
                    initializationStageFactory,
                    demandRetrievalFactory,
                    demandClassificationFactory,
                    demandTransformationFactory);

    /**
     * Test for the analysis pipeline and factory.
     */
    @Test
    public void testPipelineAndFactory() {

        // setup input
        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();
        final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

        // setup mocks
        final AtomicLong stageIdCounter = new AtomicLong(0);
        when(identityProvider.next()).thenAnswer((invocation) -> stageIdCounter.incrementAndGet());

        final AnalysisStage<Void, Void> initializationStage = mock(AnalysisStage.class);
        when(initializationStageFactory.createStage(anyLong(), any(), any())).thenReturn(initializationStage);

        final AnalysisStage<Void, EntityCloudTierDemandSet> demandRetrievalStage = mock(AnalysisStage.class);
        when(demandRetrievalFactory.createStage(anyLong(), any(), any())).thenReturn(demandRetrievalStage);

        final AnalysisStage<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> demandClassificationStage =
                mock(AnalysisStage.class);
        when(demandClassificationFactory.createStage(anyLong(), any(), any())).thenReturn(demandClassificationStage);

        final AnalysisStage<ClassifiedEntityDemandSet, AggregateAnalysisDemand> demandTransformationStage =
                mock(AnalysisStage.class);
        when(demandTransformationFactory.createStage(anyLong(), any(), any())).thenReturn(demandTransformationStage);

        // invoke pipeline factory
        final AnalysisPipeline analysisPipeline = analysisPipelineFactory.createAnalysisPipeline(
                analysisConfig, analysisContext);

        // check the invocation of the initialization stage factory
        ArgumentCaptor<Long> stageIdCaptor = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<CloudCommitmentAnalysisConfig> analysisConfigCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisConfig.class);
        ArgumentCaptor<CloudCommitmentAnalysisContext> analysisContextCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisContext.class);
        verify(initializationStageFactory).createStage(stageIdCaptor.capture(),
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());

        assertThat(stageIdCaptor.getValue(), equalTo(1L));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the invocation of the demand retrieval factory
        verify(demandRetrievalFactory).createStage(stageIdCaptor.capture(),
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());

        assertThat(stageIdCaptor.getValue(), equalTo(2L));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the invocation of the demand classification factory
        verify(demandClassificationFactory).createStage(stageIdCaptor.capture(),
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());

        assertThat(stageIdCaptor.getValue(), equalTo(3L));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the invocation of the demand transformation factory
        verify(demandTransformationFactory).createStage(stageIdCaptor.capture(),
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());

        assertThat(stageIdCaptor.getValue(), equalTo(4L));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the analysis pipeline structure
        assertThat(analysisPipeline.stages(), hasSize(4));
        assertThat(analysisPipeline.stages().get(0), equalTo(initializationStage));
        assertThat(analysisPipeline.stages().get(1), equalTo(demandRetrievalStage));
        assertThat(analysisPipeline.stages().get(2), equalTo(demandClassificationStage));
        assertThat(analysisPipeline.stages().get(3), equalTo(demandTransformationStage));
    }
}
