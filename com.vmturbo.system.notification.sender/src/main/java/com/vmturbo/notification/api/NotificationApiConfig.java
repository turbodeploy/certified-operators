package com.vmturbo.notification.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.impl.NotificationReceiver;

/**
 * Spring configuration to provide the {@link NotificationSender} integration.
 */
@Configuration
@Import({BaseKafkaProducerConfig.class})
public class NotificationApiConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean
    public IMessageSender<SystemNotification> notificationMessageSender() {
        return baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(NotificationReceiver.NOTIFICATION_TOPIC);
    }

    @Bean(destroyMethod = "shutdownNow")
    ExecutorService notificationApiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("notification-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}
