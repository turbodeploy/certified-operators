package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;

/**
 * Spring configuration to import to connecto to Action Orchestrator instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Import({BaseKafkaConsumerConfig.class})
public class ActionOrchestratorClientConfig {

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConsumerConfig;

    @Value("${actionOrchestratorHost}")
    private String actionOrchestratorHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService actionOrchestratorClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orchestrator-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<ActionOrchestratorNotification> actionOrchestratorClientMessageReceiver() {
        return baseKafkaConsumerConfig.kafkaConsumer()
                .messageReceiver(ActionOrchestratorNotificationReceiver.ACTIONS_TOPIC,
                        ActionOrchestratorNotification::parseFrom);
    }

    @Bean
    public ActionOrchestrator actionOrchestratorClient() {
        return new ActionOrchestratorNotificationReceiver(actionOrchestratorClientMessageReceiver(),
                actionOrchestratorClientThreadPool());
    }

    @Bean
    public Channel actionOrchestratorChannel() {
        return GrpcChannelFactory.newChannelBuilder(actionOrchestratorHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }
}
