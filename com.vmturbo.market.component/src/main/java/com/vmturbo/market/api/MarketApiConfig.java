package com.vmturbo.market.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.components.api.server.BroadcastWebsocketTransportManager;
import com.vmturbo.components.api.server.WebsocketNotificationSender;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Spring configuration to provide the {@link com.vmturbo.market.component.api.MarketComponent} integration.
 */
@Configuration
public class MarketApiConfig {

    @Value("${chunk.send.delay.msec:50}")
    long chunkSendDelayMs;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public MarketNotificationSender marketApi() {
        return new MarketNotificationSender(apiServerThreadPool(), chunkSendDelayMs,
                topologySender(), notificationSender());
    }

    @Bean
    public WebsocketNotificationSender<MarketComponentNotification> notificationSender() {
        return new WebsocketNotificationSender<>(apiServerThreadPool());
    }

    @Bean
    public WebsocketNotificationSender<MarketComponentNotification> topologySender() {
        return new WebsocketNotificationSender<>(apiServerThreadPool());
    }

    @Bean
    public WebsocketServerTransportManager transportManager() {
        return BroadcastWebsocketTransportManager.createTransportManager(apiServerThreadPool(),
                notificationSender(), topologySender());
    }

    /**
     * This bean configures endpoint to bind it to a specific address (path).
     *
     * @return bean
     */
    @Bean
    public ServerEndpointRegistration marketApiEndpointRegistration() {
        return new ServerEndpointRegistration(MarketComponentClient.WEBSOCKET_PATH,
                transportManager());
    }
}
