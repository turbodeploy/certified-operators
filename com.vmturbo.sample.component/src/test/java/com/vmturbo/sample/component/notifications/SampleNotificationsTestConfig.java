package com.vmturbo.sample.component.notifications;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Empty;

import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.WebsocketServerTransport;
import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.communication.WebsocketServerTransportManager.TransportHandler;
import com.vmturbo.components.api.server.NotificationProtobufEndpoint;
import com.vmturbo.components.api.server.WebsocketNotificationSender;
import com.vmturbo.sample.api.SampleNotifications.SampleNotification;
import com.vmturbo.sample.api.impl.SampleComponentNotificationReceiver;

/**
 * This is the configuration for the spring context for {@link SampleNotificationsTest}.
 *
 * We want to test that websocket notifications flow from the sample component to clients.
 * Therefore we need to stand up a minimal websocket "server" with the
 * {@link SampleComponentNotificationSender} and any supporting beans.
 *
 * See {@link SampleNotificationsTest} for how we use this configuration.
 */
@Configuration
public class SampleNotificationsTestConfig {
    /**
     * This bean scans the Spring context for websocket endpoints (such as the one
     * in {@link SampleComponentNotificationsConfig#sampleComponentNotificationSender()} and exposes them
     * to the outside world.
     */
    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }

    /**
     * This is the thread pool that the {@link SampleComponentNotificationSender} will
     * use to actually send requests to listeners.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService echoNotificationsThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("echo-test-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /**
     * This is the "backend" that the echo component uses to send notifications to
     * listeners.
     */
    @Bean
    public SampleComponentNotificationSender echoNotificationsBackend() {
        return new SampleComponentNotificationSender(notificationSender());
    }

    /**
     * This is the "backend" that the echo component uses to send notifications to
     * listeners.
     */
    @Bean
    public SampleComponentNotificationSender sampleComponentNotificationSender() {
        return new SampleComponentNotificationSender(notificationSender());
    }

    @Bean
    public WebsocketNotificationSender<SampleNotification> notificationSender() {
        return new WebsocketNotificationSender<>(echoNotificationsThreadPool());
    }

    @Bean
    public WebsocketServerTransportManager transportManager() {
        return new WebsocketServerTransportManager(new TransportHandler() {
            @Override
            public void onNewTransport(WebsocketServerTransport transport) {
                final ITransport<SampleNotification, Empty> endpoint =
                        new NotificationProtobufEndpoint<>(transport);
                notificationSender().addTransport(endpoint);
            }
        }, echoNotificationsThreadPool());
    }

    /**
     * This bean connects the websocket URL specified in {@link SampleComponentNotificationReceiver} to the
     * websocket endpoint provided by the {@link SampleComponentNotificationSender}.
     */
    @Bean
    public ServerEndpointRegistration echoApiEndpointRegistration() {
        return new ServerEndpointRegistration(SampleComponentNotificationReceiver.WEBSOCKET_PATH,
                transportManager());
    }
}
