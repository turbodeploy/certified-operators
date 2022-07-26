package com.vmturbo.cost.component.util;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.TopologyOnDemandCostChunk;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.cost.component.notification.TopologyCostSender;

/**
 * Spring configuration for integration tests.
 */
@Configuration
@EnableWebMvc
public class IntegrationTestConfig {

    /**
     * Create TopologyCostSender bean for test.
     *
     * @return TopologyCostSender
     */
    @Bean
    public TopologyCostSender topologyCostSender() {
        return new TopologyCostSender(chunkSender());
    }

    /**
     * Create.
     *
     * @return something
     */
    @Bean
    public IMessageSender<TopologyOnDemandCostChunk> chunkSender() {
        return new SenderReceiverPair<>();
    }
}
