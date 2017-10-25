package com.vmturbo.history.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.components.api.server.BroadcastWebsocketTransportManager;
import com.vmturbo.components.api.server.WebsocketNotificationSender;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;

@Configuration
public class HistoryApiConfig {

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService historyApiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public HistoryNotificationSender historyNotificationSender() {
        return new HistoryNotificationSender(notificationSender());
    }

    @Bean
    public WebsocketNotificationSender<HistoryComponentNotification> notificationSender() {
        return new WebsocketNotificationSender<>(historyApiServerThreadPool());
    }

    @Bean
    public WebsocketServerTransportManager transportManager() {
        return BroadcastWebsocketTransportManager.createTransportManager
                (historyApiServerThreadPool(), notificationSender());
    }

    @Bean
    public ServerEndpointRegistration historyApiEndpointRegistration() {
        return new ServerEndpointRegistration(HistoryComponentNotificationReceiver.WEBSOCKET_PATH,
                transportManager());
    }

    @Bean
    public StatsAvailabilityTracker statsAvailabilityTracker() {
        return new StatsAvailabilityTracker(historyNotificationSender());
    }
}
