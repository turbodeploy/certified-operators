package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.history.component.api.HistoryComponent;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;

/**
 * Spring configuration to import to connecto to History component instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
public class HistoryClientConfig {

    @Value("${historyHost}")
    private String historyHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${server.port}")
    private int httpPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public ComponentApiConnectionConfig historyConnectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(historyHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean
    public WebsocketNotificationReceiver<HistoryComponentNotification> messageReceiver() {
        return new WebsocketNotificationReceiver<>(historyConnectionConfig(),
                HistoryComponentNotificationReceiver.WEBSOCKET_PATH, threadPool(),
                HistoryComponentNotification::parseFrom);
    }

    @Bean
    public HistoryComponent historyComponent() {
        return new HistoryComponentNotificationReceiver(messageReceiver(), threadPool());
    }

    @Bean
    public Channel statsChannel() {
        return PingingChannelBuilder.forAddress(historyHost, grpcPort).usePlaintext(true).build();
    }
}
