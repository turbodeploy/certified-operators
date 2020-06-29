package com.vmturbo.cloud.commitment.analysis.runtime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.IdentityProvider;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.DemandSelectionStage.DemandSelectionFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * Tests {@link AnalysisPipelineFactory} and {@link AnalysisPipeline}
 */
public class AnalysisPipelineTest {

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final InitializationStageFactory initializationStageFactory = mock(InitializationStageFactory.class);

    private final DemandSelectionFactory demandSelectionFactory = mock(DemandSelectionFactory.class);

    private final AnalysisPipelineFactory analysisPipelineFactory =
            new AnalysisPipelineFactory(identityProvider,
                    initializationStageFactory,
                    demandSelectionFactory);


    @Test
    public void testPipelineAndFactory() {

        // setup input
        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();
        final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

        // setup mocks
        long firstStageId = 123L;
        long secondStageId = 456L;
        when(identityProvider.next()).thenReturn(firstStageId).thenReturn(secondStageId);

        final AnalysisStage<Void,Void> initializationStage = mock(AnalysisStage.class);
        when(initializationStageFactory.createStage(anyLong(), any(), any())).thenReturn(initializationStage);

        final AnalysisStage<Void,Set<EntityCloudTierMapping<?>>> demandSelectionStage = mock(AnalysisStage.class);
        when(demandSelectionFactory.createStage(anyLong(), any(), any())).thenReturn(demandSelectionStage);

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

        // check the analysis pipeline structure
        assertThat(analysisPipeline.stages(), hasSize(2));
        assertThat(analysisPipeline.stages().get(0), equalTo(initializationStage));
        assertThat(analysisPipeline.stages().get(1), equalTo(demandSelectionStage));
    }
}
