package com.vmturbo.history.api;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification;
import com.vmturbo.history.component.api.impl.ApplicationServiceHistoryNotificationReceiver;
import com.vmturbo.history.component.api.impl.HistoryComponentImpl;
import com.vmturbo.history.db.DbAccessConfig;
import com.vmturbo.history.notifications.ApplicationServiceHistoryNotificationSender;
import com.vmturbo.history.notifications.VolAttachmentDaysSender;
import com.vmturbo.history.stats.StatsConfig;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.MessageProducerHealthMonitor;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;
import com.vmturbo.history.stats.readers.ApplicationServiceDaysEmptyReader;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({BaseKafkaProducerConfig.class})
public class HistoryApiConfig {

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;


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
    public VolAttachmentDaysSender historyVolumeNotificationSender() {
        return new VolAttachmentDaysSender(historyVolumeMessageSender(), statsConfig.volumeAttachmentHistoryReader(),
                                           historyApiServerThreadPool());
    }

    @Bean
    public IMessageSender<HistoryComponentNotification> historyMessageSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(HistoryComponentNotificationReceiver.NOTIFICATION_TOPIC);
    }

    @Bean
    public IMessageSender<VolAttachmentHistoryOuterClass.VolAttachmentHistory> historyVolumeMessageSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(HistoryComponentImpl.HISTORY_VOL_NOTIFICATIONS);
    }

    @Bean
    public ApplicationServiceHistoryNotificationSender appServiceHistorySender() {
        return new ApplicationServiceHistoryNotificationSender(appSvcHistoryMessageSender(), appServiceDaysEmptyReader(),
                historyApiServerThreadPool());
    }

    @Bean
    protected IMessageSender<ApplicationServiceHistoryNotification> appSvcHistoryMessageSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(ApplicationServiceHistoryNotificationReceiver.NOTIFICATION_TOPIC);
    }

    @Bean
    protected ApplicationServiceDaysEmptyReader appServiceDaysEmptyReader() {
        try {
            return new ApplicationServiceDaysEmptyReader(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create appServiceDaysEmptyReader bean", e);
        }
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
