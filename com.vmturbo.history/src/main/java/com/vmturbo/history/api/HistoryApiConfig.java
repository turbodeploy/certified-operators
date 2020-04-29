package com.vmturbo.history.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.MessageProducerHealthMonitor;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;

@Configuration
@Import({BaseKafkaProducerConfig.class})
public class HistoryApiConfig {

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService historyApiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public HistoryNotificationSender historyNotificationSender() {
        return new HistoryNotificationSender(historyMessageSender());
    }

    @Bean
    public IMessageSender<HistoryComponentNotification> historyMessageSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(HistoryComponentNotificationReceiver.NOTIFICATION_TOPIC);
    }

    @Bean
    public StatsAvailabilityTracker statsAvailabilityTracker() {
        return new StatsAvailabilityTracker(historyNotificationSender());
    }

    @Bean
    public MessageProducerHealthMonitor messageProducerHealthMonitor() {
        return new MessageProducerHealthMonitor(kafkaProducerConfig.kafkaMessageSender());
    }
}
