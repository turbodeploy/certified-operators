package com.vmturbo.market.api;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

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
        return new MarketNotificationSender(projectedTopologySender(), actionPlanSender(),
                priceIndexSender());
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
    public IMessageSender<PriceIndexMessage> priceIndexSender() {
        return new SenderReceiverPair<>();
    }

}
