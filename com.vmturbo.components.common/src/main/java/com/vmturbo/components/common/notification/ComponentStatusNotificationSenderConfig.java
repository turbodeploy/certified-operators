package com.vmturbo.components.common.notification;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;
import com.vmturbo.component.status.api.ComponentStatusClientConfig;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;

/**
 * Configures the {@link ComponentStatusNotificationSender}, which each component uses to send
 * its status notifications.
 */
@Configuration
@Import({BaseKafkaProducerConfig.class})
@Lazy
public class ComponentStatusNotificationSenderConfig {

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    /**
     * The notification sender which actually sends the notification to the underlying channel.
     *
     * @return The {@link IMessageSender}.
     */
    @Bean
    public IMessageSender<ComponentStatusNotification> kafkaNotificationSender() {
        return baseKafkaProducerConfig.kafkaMessageSender()
            .messageSender(ComponentStatusClientConfig.COMPONENT_STATUS_TOPIC,
                ComponentStatusNotificationSender::generateMessageKey);
    }

    /**
     * The {@link ComponentStatusNotificationSender} which provides utility methods to send
     * notifications.
     *
     * @return The {@link ComponentStatusNotificationSender}.
     */
    @Bean
    public ComponentStatusNotificationSender componentStatusNotificationSender() {
        return new ComponentStatusNotificationSender(kafkaNotificationSender(), Clock.systemUTC());
    }

    /**
     * Threadpool to use to send notifications.
     *
     * @return The threadpool.
     */
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService componentNotificationThreadpool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("component-info-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}
