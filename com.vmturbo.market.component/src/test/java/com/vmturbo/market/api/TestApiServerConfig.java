package com.vmturbo.market.api;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.market.MarketNotificationSender;

/**
 * API server-side Spring configuration.
 */
@Configuration
@EnableWebMvc
public class TestApiServerConfig {

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(0);
    }

    @Bean
    public MarketNotificationSender marketNotificationSender() {
        return new MarketNotificationSender(projectedTopologySender(), projectedEntityCostSender(),
                        projectedEntityRiCoverageSender(),
                        actionPlanSender(), analysisSummarySender(), analysisStatusSender());
    }

    @Bean
    public IMessageSender<ActionPlan> actionPlanSender() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public IMessageSender<ProjectedTopology> projectedTopologySender() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public IMessageSender<ProjectedEntityCosts> projectedEntityCostSender() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public IMessageSender<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageSender() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public IMessageSender<AnalysisSummary> analysisSummarySender() {
        return new SenderReceiverPair<>();
    }

    /**
     * Sender for status of a market analysis run.
     *
     * @return The sender.
     */
    @Bean
    public IMessageSender<AnalysisStatusNotification> analysisStatusSender() {
        return new SenderReceiverPair<>();
    }
}
