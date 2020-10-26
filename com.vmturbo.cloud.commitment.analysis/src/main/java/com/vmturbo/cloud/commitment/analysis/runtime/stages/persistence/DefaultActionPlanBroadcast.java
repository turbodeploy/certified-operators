package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * The default implementation of {@link ActionPlanBroadcast}, publishing the {@link ActionPlan} protobuf
 * message on a kafka bus.
 */
public class DefaultActionPlanBroadcast extends
        ComponentNotificationSender<ActionPlan> implements ActionPlanBroadcast {

    private final IMessageSender<ActionPlan> actionPlanSender;

    private final RecommendationActionTranslator recommendationActionTranslator;

    /**
     * Constructs a new {@link DefaultActionPlanBroadcast} instance.
     * @param actionPlanSender The kafka {@link ActionPlan} sender.
     * @param recommendationActionTranslator A translator for {@link CloudCommitmentRecommendation} instances
     *                                       to {@link Action} instances.
     */
    public DefaultActionPlanBroadcast(@Nonnull IMessageSender<ActionPlan> actionPlanSender,
                                      @Nonnull RecommendationActionTranslator recommendationActionTranslator) {
        this.actionPlanSender = Objects.requireNonNull(actionPlanSender);
        this.recommendationActionTranslator = Objects.requireNonNull(recommendationActionTranslator);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void sendNotification(@Nonnull final CloudCommitmentAnalysisInfo analysisInfo,
                                 @Nonnull final List<CloudCommitmentRecommendation> recommendations) throws CommunicationException, InterruptedException {

        final ActionPlan actionPlan = createActionPlan(analysisInfo, recommendations);
        sendMessage(actionPlanSender, actionPlan);
    }

    @Override
    protected String describeMessage(@Nonnull final ActionPlan actionPlan) {
        return ActionPlan.class.getSimpleName() + "[" + actionPlan.getInfo() + "]";
    }

    private ActionPlan createActionPlan(@Nonnull final CloudCommitmentAnalysisInfo analysisInfo,
                                        @Nonnull final List<CloudCommitmentRecommendation> recommendations) {

        final List<Action> actions = recommendations.stream()
                // filter out recommendations for zero RIs
                .filter(CloudCommitmentRecommendation::isActionable)
                .map(recommendationActionTranslator::translateRecommendation)
                .collect(ImmutableList.toImmutableList());

        return ActionPlan.newBuilder()
                .setId(IdentityGenerator.next())
                .setAnalysisStartTimestamp(analysisInfo.getCreationTime())
                .setAnalysisCompleteTimestamp(Instant.now().toEpochMilli())
                .setInfo(ActionPlanInfo.newBuilder()
                        .setBuyRi(BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(analysisInfo.getAnalysisTopology()
                                        .getTopologyContextId())))
                .addAllAction(actions)
                .build();
    }
}
