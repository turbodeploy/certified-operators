package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.ResultsPersistenceStage.ResultsPersistenceFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendationTest;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendations;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;

public class ResultsPersistenceStageTest {

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final CloudCommitmentRecommendationStore recommendationStore =
            mock(CloudCommitmentRecommendationStore.class);

    private final ActionPlanBroadcast actionPlanBroadcast = mock(ActionPlanBroadcast.class);

    private final ResultsPersistenceFactory resultsPersistenceFactory = new ResultsPersistenceFactory(
            recommendationStore, actionPlanBroadcast);

    private final CloudCommitmentRecommendations cloudCommitmentRecommendations = CloudCommitmentRecommendations.builder()
            .analysisTopology(mock(AnalysisTopology.class))
            .addCommitmentRecommendations(CloudCommitmentRecommendationTest.RESERVED_INSTANCE_RECOMMENDATION)
            .build();


    @Test
    public void testPersistingResults() throws Exception {

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(2L)
                .setCreationTime(Instant.now().toEpochMilli())
                .setAnalysisTopology(TopologyReference.newBuilder()
                        .setTopologyContextId(123L)
                        .build())
                .build();
        when(analysisContext.getAnalysisInfo()).thenReturn(analysisInfo);

        final AnalysisStage<CloudCommitmentRecommendations, CloudCommitmentRecommendations> persistenceStage =
                resultsPersistenceFactory.createStage(1L,
                        TestUtils.createBaseConfig(),
                        analysisContext);

        // invoke the stage
        persistenceStage.execute(cloudCommitmentRecommendations);

        // Capture and assert on recommendation store
        final ArgumentCaptor<CloudCommitmentAnalysisInfo> analysisInfoCaptor =
                ArgumentCaptor.forClass(CloudCommitmentAnalysisInfo.class);
        final ArgumentCaptor<Collection> recommendationCollectionCaptor =
                ArgumentCaptor.forClass(Collection.class);
        verify(recommendationStore).persistRecommendations(
                analysisInfoCaptor.capture(),
                recommendationCollectionCaptor.capture(),
                any());
        assertThat(analysisInfoCaptor.getValue(), equalTo(analysisInfo));
        assertThat(recommendationCollectionCaptor.getValue(),
                equalTo(cloudCommitmentRecommendations.commitmentRecommendations()));

        // Capture and assert on action plan broadcast
        final ArgumentCaptor<List> recommendationListCaptor =
                ArgumentCaptor.forClass(List.class);
        verify(actionPlanBroadcast).sendNotification(
                analysisInfoCaptor.capture(),
                recommendationListCaptor.capture());
        assertThat(analysisInfoCaptor.getValue(), equalTo(analysisInfo));
        assertThat(recommendationListCaptor.getValue(),
                equalTo(cloudCommitmentRecommendations.commitmentRecommendations()));

    }

    @Test
    public void testSkippingPersistence() throws Exception {

        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(2L)
                .setCreationTime(Instant.now().toEpochMilli())
                .build();
        when(analysisContext.getAnalysisInfo()).thenReturn(analysisInfo);

        final AnalysisStage<CloudCommitmentRecommendations, CloudCommitmentRecommendations> persistenceStage =
                resultsPersistenceFactory.createStage(1L,
                        TestUtils.createBaseConfig(),
                        analysisContext);

        // invoke the stage
        persistenceStage.execute(cloudCommitmentRecommendations);

        // Verify neither store nor plan broadcast is invoked
        verify(recommendationStore, times(0)).persistRecommendations(
                any(), any(), any());
        verify(actionPlanBroadcast, times(0)).sendNotification(any(), any());
    }
}
