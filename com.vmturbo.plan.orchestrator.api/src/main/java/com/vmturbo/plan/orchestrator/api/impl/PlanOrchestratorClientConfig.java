package com.vmturbo.plan.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.api.PlanOrchestratorDTO.PlanNotification;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;

/**
 * Spring configuration to import to connect to Action Orchestrator instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
public class PlanOrchestratorClientConfig {

    @Value("${planOrchestratorHost}")
    private String planOrchestratorHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    protected ComponentApiConnectionConfig connectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(planOrchestratorHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService planOrchestratorListenerThreadpool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "plan-orchestrator-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<PlanNotification> messageReceiver() {
        return new WebsocketNotificationReceiver<>(connectionConfig(),
                PlanOrchestratorClientImpl.WEBSOCKET_PATH,
                planOrchestratorListenerThreadpool(), PlanNotification::parseFrom);
    }

    @Bean
    public PlanOrchestrator planOrchestrator() {
        return new PlanOrchestratorClientImpl(messageReceiver(),
                planOrchestratorListenerThreadpool());
    }

    @Bean
    public Channel planOrchestratorChannel() {
        return PingingChannelBuilder.forAddress(planOrchestratorHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }
}
