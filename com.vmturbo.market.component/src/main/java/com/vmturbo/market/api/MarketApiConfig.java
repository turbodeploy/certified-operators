package com.vmturbo.market.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.market.component.api.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

/**
 * Spring configuration to provide the {@link com.vmturbo.market.component.api.MarketComponent} integration.
 */
@Configuration
@Import(BaseKafkaProducerConfig.class)
public class MarketApiConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public MarketNotificationSender marketApi() {
        return MarketKafkaSender.createMarketSender(apiServerThreadPool(),
                baseKafkaProducerConfig.kafkaMessageSender());
    }

    @Bean
    public IMessageSender<ActionPlan> actionPlanSender() {
        return baseKafkaProducerConfig.kafkaMessageSender().messageSender(MarketComponentClient.ACTION_PLANS_TOPIC);
    }

    @Bean
    public IMessageSender<ProjectedTopology> projectedTopologySender() {
        return baseKafkaProducerConfig.kafkaMessageSender().messageSender(MarketComponentClient.PROJECTED_TOPOLOGIES_TOPIC);
    }

}
