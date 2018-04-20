package com.vmturbo.sample.component.notifications;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
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
@Import({BaseKafkaProducerConfig.class})
public class SampleComponentNotificationsConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

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
        return new SampleComponentNotificationSender(notificationSender());
    }

    @Bean
    public IMessageSender<SampleNotification> notificationSender() {
        return baseKafkaProducerConfig.kafkaMessageSender().messageSender("sample-topic");
    }
}
