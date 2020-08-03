package com.vmturbo.cloud.commitment.analysis.runtime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage.StageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.CloudCommitmentAnalysisException;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.IdentityProvider;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext.AnalysisContextFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * Test both {@link AnalysisFactory} and {@link CloudCommitmentAnalysis}.
 */
public class CloudCommitmentAnalysisTest {

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final long stageAId = 2;
    private final String stageAName = "STAGE_A";
    private final AnalysisStage<Void, Object> stageA = mock(AnalysisStage.class);

    private final long stageBId = 3;
    private final String stageBName = "STAGE_B";
    private final AnalysisStage<Object, Object> stageB = mock(AnalysisStage.class);

    private final long stageCId = 4;
    private final String stageCName = "STAGE_C";
    private final AnalysisStage<Object, Object> stageC = mock(AnalysisStage.class);

    private final AnalysisPipeline analysisPipeline = AnalysisPipeline.newBuilder()
            .addStage(stageA)
            .addStage(stageB)
            .addStage(stageC)
            .build();

    private final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();

    private final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
            .setOid(1L)
            .setCreationTime(Instant.now().toEpochMilli())
            .build();

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final AnalysisPipelineFactory analysisPipelineFactory = mock(AnalysisPipelineFactory.class);

    private final AnalysisContextFactory analysisContextFactory = mock(AnalysisContextFactory.class);

    private final AnalysisFactory analysisFactory = new AnalysisFactory(
            identityProvider,
            analysisPipelineFactory,
            analysisContextFactory);

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        when(analysisContext.getAnalysisInfo()).thenReturn(analysisInfo);

        // setup the stages
        when(stageA.id()).thenReturn(stageAId);
        when(stageA.stageName()).thenReturn(stageAName);

        when(stageB.id()).thenReturn(stageBId);
        when(stageB.stageName()).thenReturn(stageBName);

        when(stageC.id()).thenReturn(stageCId);
        when(stageC.stageName()).thenReturn(stageCName);

        when(analysisContextFactory.createContext(any(), any())).thenReturn(analysisContext);
        when(analysisPipelineFactory.createAnalysisPipeline(any(), any())).thenReturn(analysisPipeline);

    }

    /**
     * Test that the pipeline was successfull.
     *
     * @throws CloudCommitmentAnalysisException A cloud commitment analysis exception.
     */
    @Test
    public void testSuccessfulPipeline() throws CloudCommitmentAnalysisException {

        // setup stage output
        final Object stageAOutput = new Object();
        final StageResult<Object> stageAResult = ImmutableStageResult.builder()
                .output(stageAOutput)
                .resultSummary("stageASummery")
                .build();
        when(stageA.execute(any())).thenReturn(stageAResult);

        final Object stageBOutput = new Object();
        final StageResult<Object> stageBResult = ImmutableStageResult.builder()
                .output(stageBOutput)
                .resultSummary("stageBSummery")
                .build();
        when(stageB.execute(any())).thenReturn(stageBResult);

        final Object stageCOutput = new Object();
        final StageResult<Object> stageCResult = ImmutableStageResult.builder()
                .output(stageCOutput)
                .resultSummary("stageCSummery")
                .build();
        when(stageC.execute(any())).thenReturn(stageCResult);


        // run the analysis
        final CloudCommitmentAnalysis cloudCommitmentAnalysis = analysisFactory.createAnalysis(analysisConfig);
        cloudCommitmentAnalysis.run();

        // check analysis factory
        verify(analysisContextFactory).createContext(any(), any());

        final ArgumentCaptor<CloudCommitmentAnalysisConfig> analysisConfigCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisConfig.class);
        final ArgumentCaptor<CloudCommitmentAnalysisContext> analysisContextCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisContext.class);
        verify(analysisPipelineFactory).createAnalysisPipeline(
                analysisConfigCaptor.capture(),
                analysisContextCaptor.capture());
        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));
        assertThat(analysisContextCaptor.getValue(), equalTo(analysisContext));

        // check stage inputs
        final ArgumentCaptor<Void> stageAInput = ArgumentCaptor.forClass(Void.class);
        verify(stageA).execute(stageAInput.capture());

        final ArgumentCaptor<Object> stageBInput = ArgumentCaptor.forClass(Object.class);
        verify(stageB).execute(stageBInput.capture());

        final ArgumentCaptor<Object> stageCInput = ArgumentCaptor.forClass(Object.class);
        verify(stageC).execute(stageCInput.capture());

        assertThat(stageBInput.getValue(), equalTo(stageAOutput));
        assertThat(stageCInput.getValue(), equalTo(stageBOutput));
    }

    /**
     * Test failure of pipeline.
     *
     * @throws CloudCommitmentAnalysisException A cloud commitment analysis exception.
     */
    @Test(expected = CloudCommitmentAnalysisException.class)
    public void testFailedPipeline() throws CloudCommitmentAnalysisException {

        // setup stage output
        final Object stageAOutput = new Object();
        final StageResult<Object> stageAResult = ImmutableStageResult.builder()
                .output(stageAOutput)
                .resultSummary("stageASummery")
                .build();
        when(stageA.execute(any())).thenReturn(stageAResult);

        final Object stageBOutput = new Object();
        final StageResult<Object> stageBResult = ImmutableStageResult.builder()
                .output(stageBOutput)
                .resultSummary("stageBSummery")
                .build();
        when(stageB.execute(any())).thenReturn(stageBResult);

        final Object stageCOutput = new Object();
        final StageResult<Object> stageCResult = ImmutableStageResult.builder()
                .output(stageCOutput)
                .resultSummary("stageCSummery")
                .build();
        when(stageC.execute(any())).thenThrow(new RuntimeException(("Stage C Exception")));

        // run the analysis
        final CloudCommitmentAnalysis cloudCommitmentAnalysis = analysisFactory.createAnalysis(analysisConfig);
        cloudCommitmentAnalysis.run();
    }
}
