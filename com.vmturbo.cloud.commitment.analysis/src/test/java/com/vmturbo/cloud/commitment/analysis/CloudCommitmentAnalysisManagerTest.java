package com.vmturbo.cloud.commitment.analysis;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysis.CloudCommitmentAnalysisException;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * Class for testing the cloud commitment analysis manager.
 */
public class CloudCommitmentAnalysisManagerTest {

    private final AnalysisFactory analysisFactory = mock(AnalysisFactory.class);

    private final TaskScheduler taskScheduler = mock(TaskScheduler.class);

    private final Duration logDuration = Duration.ofMillis(1234);

    private CloudCommitmentAnalysisManager analysisManager;

    /**
     * Setup the test.
     */
    @Before
    public void setup() {

        analysisManager = new CloudCommitmentAnalysisManager(
                analysisFactory,
                taskScheduler,
                logDuration,
                5);
    }

    /**
     * Test construction of the log duration.
     */
    @Test
    public void testConstruction() {

        final ArgumentCaptor<Duration> logDurationCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(taskScheduler).scheduleWithFixedDelay(any(), logDurationCaptor.capture());

        assertThat(logDurationCaptor.getValue(), equalTo(logDuration));
    }

    /**
     * Test that the analysis was successfull.
     *
     * @throws CloudCommitmentAnalysisException A cloud commitment analysis exception
     */
    @Test
    public void testSuccessfulAnalysis() throws CloudCommitmentAnalysisException {

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(123L)
                .setCreationTime(Instant.now().toEpochMilli())
                .build();

        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig();
        final CloudCommitmentAnalysis analysis = mock(CloudCommitmentAnalysis.class);
        when(analysis.info()).thenReturn(analysisInfo);
        when(analysisFactory.createAnalysis(any())).thenReturn(analysis);


        // create analysis
        final CloudCommitmentAnalysisInfo actualAnalysisInfo = analysisManager.startAnalysis(analysisConfig);

        // verify config passed to analysis factory
        final ArgumentCaptor<CloudCommitmentAnalysisConfig> analysisConfigCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisConfig.class);
        verify(analysisFactory).createAnalysis(analysisConfigCaptor.capture());

        assertThat(analysisConfigCaptor.getValue(), equalTo(analysisConfig));

        // verify the returned analysis info is correct
        assertThat(actualAnalysisInfo, equalTo(analysisInfo));

        // verify the analysis was run
        verify(analysis, timeout(10000)).run();

    }
}
