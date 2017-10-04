package com.vmturbo.market.component.api.impl;

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
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Spring configuration to import to connecto to Market instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
public class MarketClientConfig {

    @Value("${marketHost}")
    private String marketHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Bean
    protected IMessageReceiver<MarketComponentNotification> marketClientNotificationReceiver() {
        return new MarketMessageReceiver(marketClientConnectionConfig(), marketClientThreadPool());
    }

    @Bean
    protected ComponentApiConnectionConfig marketClientConnectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(marketHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
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
                marketClientThreadPool(), marketClientNotificationReceiver());
    }
}
