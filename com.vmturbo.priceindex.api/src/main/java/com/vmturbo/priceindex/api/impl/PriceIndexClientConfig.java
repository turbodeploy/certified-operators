package com.vmturbo.priceindex.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

@Configuration
@Lazy
public class PriceIndexClientConfig {
    @Value("${marketHost}")
    private String marketHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService priceIndexClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("priceindex-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected ComponentApiConnectionConfig priceIndexClientConnectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(marketHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean
    protected IMessageReceiver<PriceIndexMessage> priceIndexClientMessageReceiver() {
        return new WebsocketNotificationReceiver<>(priceIndexClientConnectionConfig(),
                PriceIndexReceiver.WEBSOCKET_PATH, priceIndexClientThreadPool(),
                PriceIndexMessage::parseFrom);
    }

    @Bean
    public PriceIndexReceiver priceIndexReceiver() {
        return PriceIndexReceiver.rpcAndNotification(priceIndexClientConnectionConfig(),
                priceIndexClientThreadPool(),
                priceIndexClientMessageReceiver());
    }
}
