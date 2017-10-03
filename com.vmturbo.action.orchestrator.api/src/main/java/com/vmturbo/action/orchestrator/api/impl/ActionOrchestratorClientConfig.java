package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;

/**
 * Spring configuration to import to connecto to Action Orchestrator instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
public class ActionOrchestratorClientConfig {

    @Value("${actionOrchestratorHost}")
    private String actionOrchestratorHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orchestrator-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected ComponentApiConnectionConfig connectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(actionOrchestratorHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean
    protected IMessageReceiver<ActionOrchestratorNotification> messageReceiver() {
        return new WebsocketNotificationReceiver<>(connectionConfig(),
                ActionOrchestratorClient.WEBSOCKET_PATH, threadPool(),
                ActionOrchestratorNotification::parseFrom);
    }

    @Bean
    public ActionOrchestratorClient actionOrchestratorClient() {
        return ActionOrchestratorClient.rpcAndNotification(connectionConfig(), threadPool(),
                messageReceiver());
    }

    @Bean
    public Channel actionOrchestratorChannel() {
        return PingingChannelBuilder.forAddress(actionOrchestratorHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }
}
