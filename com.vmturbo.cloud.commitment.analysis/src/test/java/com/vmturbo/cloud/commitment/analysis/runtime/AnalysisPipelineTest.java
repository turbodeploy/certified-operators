package com.vmturbo.cloud.commitment.analysis.runtime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.IdentityProvider;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.selection.DemandSelectionStage.DemandSelectionFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.selection.EntityCloudTierDemandSet;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * Tests {@link AnalysisPipelineFactory} and {@link AnalysisPipeline}.
 */
public class AnalysisPipelineTest {

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final InitializationStageFactory initializationStageFactory = mock(InitializationStageFactory.class);

    private final DemandSelectionFactory demandSelectionFactory = mock(DemandSelectionFactory.class);

    private final DemandClassificationFactory demandClassificationFactory = mock(DemandClassificationFactory.class);

    private final AnalysisPipelineFactory analysisPipelineFactory =
            new AnalysisPipelineFactory(identityProvider,
                    initializationStageFactory,
                    demandSelectionFactory,
                    demandClassificationFactory);

    /**
     * Test for the analysis pipeline and factory.
     */
    @Test
    public void testPipelineAndFactory() {

        // setup input
        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();
        final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

        // setup mocks
        long firstStageId = 123L;
        long secondStageId = 456L;
        long thirdStageId = 789L;
        when(identityProvider.next()).thenReturn(firstStageId)
                .thenReturn(secondStageId)
                .thenReturn(thirdStageId);

        final AnalysisStage<Void, Void> initializationStage = mock(AnalysisStage.class);
        when(initializationStageFactory.createStage(anyLong(), any(), any())).thenReturn(initializationStage);

        final AnalysisStage<Void, EntityCloudTierDemandSet> demandSelectionStage = mock(AnalysisStage.class);
        when(demandSelectionFactory.createStage(anyLong(), any(), any())).thenReturn(demandSelectionStage);

        final AnalysisStage<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> demandClassificationStage =
                mock(AnalysisStage.class);
        when(demandClassificationFactory.createStage(anyLong(), any(), any())).thenReturn(demandClassificationStage);

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

        assertThat(stageIdCaptor.getValue(), equalTo(firstStageId));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the invocation of the demand selection factory
        verify(demandSelectionFactory).createStage(stageIdCaptor.capture(),
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());

        assertThat(stageIdCaptor.getValue(), equalTo(secondStageId));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the invocation of the demand classification factory
        verify(demandClassificationFactory).createStage(stageIdCaptor.capture(),
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());

        assertThat(stageIdCaptor.getValue(), equalTo(thirdStageId));
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo((analysisContext)));

        // check the analysis pipeline structure
        assertThat(analysisPipeline.stages(), hasSize(3));
        assertThat(analysisPipeline.stages().get(0), equalTo(initializationStage));
        assertThat(analysisPipeline.stages().get(1), equalTo(demandSelectionStage));
        assertThat(analysisPipeline.stages().get(2), equalTo(demandClassificationStage));
    }
}
