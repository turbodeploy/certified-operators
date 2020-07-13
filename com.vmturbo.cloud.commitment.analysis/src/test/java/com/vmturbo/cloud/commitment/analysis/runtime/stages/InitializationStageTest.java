package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage.StageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.InitializationStage.InitializationStageFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;

/**
 * Class for testing the initialization stage.
 */
public class InitializationStageTest {

    private final long id = 123L;

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final InitializationStageFactory initializationStageFactory = new InitializationStageFactory();

    /**
     * Testing stage execution.
     */
    @Test
    public void testStageExecution() {

        final Instant historicalLoobackStartTime = Instant.ofEpochSecond(90 * 60);

        final CloudCommitmentAnalysisConfig.Builder analysisConfigBuilder = TestUtils.createBaseConfig().toBuilder();
        analysisConfigBuilder.getDemandSelectionBuilder()
                .setLookBackStartTime(historicalLoobackStartTime.toEpochMilli());
        final CloudCommitmentAnalysisConfig analysisConfig = analysisConfigBuilder.build();

        final AnalysisStage initializationStage = initializationStageFactory.createStage(
                id,
                analysisConfig,
                analysisContext);

        when(analysisContext.getAnalysisSegmentUnit()).thenReturn(ChronoUnit.HOURS);

        final StageResult stageResult = initializationStage.execute(null);

        final ArgumentCaptor<Instant> instantArgumentCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(analysisContext).setAnalysisStartTime(instantArgumentCaptor.capture());

        // check assertions
        final StageResult expectedStageResult = ImmutableStageResult.builder()
                .output(null)
                .build();
        final Instant expectedAnalysisStartTime = Instant.ofEpochSecond(60 * 60);

        assertThat(stageResult, equalTo(expectedStageResult));
        assertThat(instantArgumentCaptor.getValue(), equalTo(expectedAnalysisStartTime));
    }
}
