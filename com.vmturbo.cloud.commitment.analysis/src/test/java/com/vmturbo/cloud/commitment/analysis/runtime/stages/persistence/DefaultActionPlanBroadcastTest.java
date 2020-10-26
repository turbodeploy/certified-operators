package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendationTest;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;

public class DefaultActionPlanBroadcastTest {

    private final IMessageSender<ActionPlan> actionPlanSender = mock(IMessageSender.class);

    private final RecommendationActionTranslator actionTranslator = new RecommendationActionTranslator();

    private DefaultActionPlanBroadcast actionPlanBroadcast;

    @Before
    public void setup() {
        actionPlanBroadcast = new DefaultActionPlanBroadcast(actionPlanSender, actionTranslator);
    }

    @Test
    public void testActionPlan() throws CommunicationException, InterruptedException {

        // setup the input
        final CloudCommitmentAnalysisInfo analysisInfo = CloudCommitmentAnalysisInfo.newBuilder()
                .setOid(2L)
                .setCreationTime(Instant.now().toEpochMilli())
                .setAnalysisTopology(TopologyReference.newBuilder()
                        .setTopologyContextId(123L)
                        .build())
                .build();
        final List<CloudCommitmentRecommendation> recommendations = ImmutableList.of(
                CloudCommitmentRecommendationTest.RESERVED_INSTANCE_RECOMMENDATION);

        // Invoke the action broadcast
        actionPlanBroadcast.sendNotification(analysisInfo, recommendations);

        final ArgumentCaptor<ActionPlan> actionPlanCaptor = ArgumentCaptor.forClass(ActionPlan.class);
        verify(actionPlanSender).sendMessage(actionPlanCaptor.capture());

        // ASSERTIONS
        final ActionPlan actionPlan = actionPlanCaptor.getValue();
        assertTrue(actionPlan.getInfo().hasBuyRi());
        assertThat(actionPlan.getActionList(), hasSize(1));
    }
}
