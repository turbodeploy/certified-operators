package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.ActionPlanBroadcast;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.CloudCommitmentRecommendationStore;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.DefaultActionPlanBroadcast;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.RecommendationActionTranslator;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence.ResultsPersistenceStage.ResultsPersistenceFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Spring configuration for creating a {@link ResultsPersistenceFactory} instance.
 */
@Lazy
@Configuration
public class ResultsPersistenceConfig {

    @Lazy
    @Autowired
    private CloudCommitmentRecommendationStore cloudCommitmentRecommendationStore;

    @Lazy
    @Autowired
    private IMessageSender<ActionPlan> actionPlanSender;

    /**
     * The {@link RecommendationActionTranslator}.
     * @return The {@link RecommendationActionTranslator}.
     */
    @Bean
    public RecommendationActionTranslator recommendationActionTranslator() {
        return new RecommendationActionTranslator();
    }

    /**
     * The {@link ActionPlanBroadcast}.
     * @return The {@link ActionPlanBroadcast}.
     */
    @Bean
    public ActionPlanBroadcast actionPlanBroadcast() {
        return new DefaultActionPlanBroadcast(
                actionPlanSender,
                recommendationActionTranslator());
    }

    /**
     * The {@link ResultsPersistenceFactory}.
     * @return The {@link ResultsPersistenceFactory}.
     */
    @Bean
    public ResultsPersistenceFactory resultsPersistenceFactory() {
        return new ResultsPersistenceFactory(
                cloudCommitmentRecommendationStore,
                actionPlanBroadcast());
    }
}
