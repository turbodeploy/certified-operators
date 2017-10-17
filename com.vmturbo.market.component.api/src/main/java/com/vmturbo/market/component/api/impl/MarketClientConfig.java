package com.vmturbo.market.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.MarketComponent;

/**
 * Spring configuration to import to connecto to Market instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
@Import(BaseKafkaConsumerConfig.class)
public class MarketClientConfig {

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConfig;

    @Value("${marketHost}")
    private String marketHost;

    @Value("${server.port}")
    private int httpPort;

    @Bean
    protected IMessageReceiver<ActionPlan> actionPlanReceiver() {
        return baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentClient.ACTION_PLANS_TOPIC,
                ActionPlan::parseFrom);
    }

    @Bean
    protected IMessageReceiver<ProjectedTopology> projectedTopologyReceiver() {
        return baseKafkaConfig.kafkaConsumer().messageReceiver(
                MarketComponentClient.PROJECTED_TOPOLOGIES_TOPIC,
                ProjectedTopology::parseFrom);
    }

    @Bean
    protected ComponentApiConnectionConfig marketClientConnectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(marketHost, httpPort)
                .build();
    }

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService marketClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public MarketComponent marketComponent() {
        return MarketComponentClient.rpcAndNotification(marketClientConnectionConfig(),
                marketClientThreadPool(), projectedTopologyReceiver(), actionPlanReceiver());
    }
}
