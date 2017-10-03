package com.vmturbo.repository.api.impl;

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

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.RepositoryDTO.RepositoryNotification;

@Configuration
@Lazy
public class RepositoryClientConfig {

    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Value("${server.port}")
    private int httpPort;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("repository-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public ComponentApiConnectionConfig repositoryConnectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(repositoryHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean
    protected IMessageReceiver<RepositoryNotification> messageReceiver() {
        return new WebsocketNotificationReceiver<>(repositoryConnectionConfig(),
                RepositoryNotificationReceiver.WEBSOCKET_PATH, threadPool(),
                RepositoryNotification::parseFrom);
    }

    @Bean
    public Repository repository() {
        return new RepositoryNotificationReceiver(messageReceiver(), threadPool());
    }

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel repositoryChannel() {
        return PingingChannelBuilder.forAddress(repositoryHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public RepositoryClient repositoryClient() {
        return new RepositoryClient(repositoryChannel());
    }
}
