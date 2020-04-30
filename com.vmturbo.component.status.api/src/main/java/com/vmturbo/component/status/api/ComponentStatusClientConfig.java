package com.vmturbo.component.status.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;

/**
 * If you want to listen to component status notifications (components starting up and going down),
 * import this configuration.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class})
public class ComponentStatusClientConfig {

    /**
     * The name of the kafka topic to which component status notifications go.
     */
    public static final String COMPONENT_STATUS_TOPIC = "component-status";

    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;


    @Bean(destroyMethod = "shutdownNow")
    ExecutorService notificationClientThreadPool() {
        final ThreadFactory threadFactory =
            new ThreadFactoryBuilder().setNameFormat("notification-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    IMessageReceiver<ComponentStatusNotification> notificationClientMessageReceiver() {
        return kafkaConsumerConfig.kafkaConsumer()
            .messageReceiverWithSettings(
                new TopicSettings(COMPONENT_STATUS_TOPIC, StartFrom.BEGINNING),
                ComponentStatusNotification::parseFrom);
    }

    /**
     * Receiver for component status notifications.
     * Add your listener to it via {@link ComponentStatusNotificationReceiver#addListener}.
     *
     * @return The {@link ComponentStatusNotificationReceiver}.
     */
    @Bean
    public ComponentStatusNotificationReceiver componentStatusNotificationReceiver() {
        return new ComponentStatusNotificationReceiver(notificationClientMessageReceiver(),
            notificationClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }
}
