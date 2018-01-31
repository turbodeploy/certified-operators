package com.vmturbo.repository;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

/**
 * Spring configuration for API-related beans
 */
@Configuration
@Import({BaseKafkaProducerConfig.class, RepositoryClientConfig.class})
public class RepositoryApiConfig {

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("repository-notify-api-%d")
                        .build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public RepositoryNotificationSender repositoryNotificationSender() {
        return new RepositoryNotificationSender(notificationSender());
    }

    @Bean
    public IMessageSender<RepositoryNotification> notificationSender() {
        return kafkaProducerConfig.kafkaMessageSender().messageSender
                (RepositoryNotificationReceiver.TOPOLOGY_TOPIC);
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaHealthMonitor() {
        return new KafkaProducerHealthMonitor(kafkaProducerConfig.kafkaMessageSender());
    }
}
