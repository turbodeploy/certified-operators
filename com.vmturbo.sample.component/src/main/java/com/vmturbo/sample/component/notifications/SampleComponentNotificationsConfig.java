package com.vmturbo.sample.component.notifications;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
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
import com.vmturbo.sample.component.SampleComponent;

/**
 * Configuration for server-side support of notifications over
 * websocket.
 *
 * The {@link SampleComponentNotificationSender} creates a websocket endpoint. We then
 * use a {@link ServerEndpointRegistration} bean to make that endpoint available at the
 * path specified in {@link SampleComponentNotificationReceiver#WEBSOCKET_PATH}.
 *
 * The {@link ServerEndpointExporter} bean (created in {@link SampleComponent}) scans the context
 * for all registrations and registers them with the Java WebSocket runtime.
 */
@Configuration
public class SampleComponentNotificationsConfig {

    /**
     * Spring will inject this value from Consul during context initialization at startup.
     * The defaults are listed in
     * com.vmturbo.clustermgr/src/main/resources/factoryInstalledComponents.yml.
     *
     * IMPORTANT: Use @Value annotations only in configuration classes, and pass them to
     *  the classes that need them via constructor properties.
     */
    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeoutMs;

    /**
     * This is the thread pool that the {@link SampleComponentNotificationSender} will
     * use to actually send requests to listeners.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService echoNotificationsThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("echo-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /**
     * This is the "backend" that the echo component uses to send notifications to
     * listeners.
     */
    @Bean
    public SampleComponentNotificationSender sampleComponentNotificationSender() {
        return new SampleComponentNotificationSender(echoNotificationsThreadPool(),
                notificationSender());
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
