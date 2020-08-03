package com.vmturbo.notification.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;

/**
 * Spring configuration to import to connect to component instance that provide system notification.
 */
@Configuration
@Import({BaseKafkaConsumerConfig.class})
public class NotificationClientConfig {

    public static final String SYSTEM_NOTIFICATION_TOPIC = "system-notifications";

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
    IMessageReceiver<SystemNotification> notificationClientMessageReceiver() {
        return kafkaConsumerConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                        new TopicSettings(SYSTEM_NOTIFICATION_TOPIC, StartFrom.BEGINNING),
                        SystemNotification::parseFrom);
    }

    @Bean
    public NotificationReceiver systemNotificationListener() {
        return new NotificationReceiver(notificationClientMessageReceiver(),
                notificationClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }
}
