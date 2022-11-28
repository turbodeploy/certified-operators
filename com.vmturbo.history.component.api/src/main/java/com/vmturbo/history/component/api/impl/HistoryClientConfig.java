package com.vmturbo.history.component.api.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;

import javax.annotation.Nonnull;

/**
 * Spring configuration to import to connecto to History component instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class})
public class HistoryClientConfig {

    @Value("${historyHost}")
    private String historyHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;


    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService historyClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public IMessageReceiver<HistoryComponentNotification> historyClientMessageReceiver() {
        return HistoryMessageReceiver.create(kafkaConsumerConfig.kafkaConsumer());
    }

    @Bean
    public HistoryComponentNotificationReceiver historyComponent() {
        return new HistoryComponentNotificationReceiver(historyClientMessageReceiver(),
                historyClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }

    @Bean
    public ApplicationServiceHistoryNotificationReceiver appServiceHistoryNotificationReceiver() {
        return new ApplicationServiceHistoryNotificationReceiver(appServiceHistoryMessageReceiver(),
                historyClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }

    @Bean
    protected IMessageReceiver<ApplicationServiceHistoryNotification> appServiceHistoryMessageReceiver() {
        return kafkaConsumerConfig.kafkaConsumer().messageReceiver(
                ApplicationServiceHistoryNotificationReceiver.NOTIFICATION_TOPIC,
                ApplicationServiceHistoryNotification::parseFrom);
    }

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService HistoryNotificationClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("hist-client-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
    @Bean
    protected IMessageReceiver<VolAttachmentHistoryOuterClass.VolAttachmentHistory> historyNotificationReceiver(
            @Nonnull final Optional<KafkaMessageConsumer.TopicSettings.StartFrom> startFromOverride) {
        return startFromOverride
                .map(startFrom -> kafkaConsumerConfig.kafkaConsumer().messageReceiverWithSettings(
                        new KafkaMessageConsumer.TopicSettings(HistoryComponentImpl.HISTORY_VOL_NOTIFICATIONS, startFrom),
                        VolAttachmentHistoryOuterClass.VolAttachmentHistory::parseFrom))
                .orElseGet(() -> kafkaConsumerConfig.kafkaConsumer().messageReceiver(
                        HistoryComponentImpl.HISTORY_VOL_NOTIFICATIONS,
                        VolAttachmentHistoryOuterClass.VolAttachmentHistory::parseFrom));
    }

    /**
     * The returns the history component for adding listeners.
     *
     * @param subscriptions The set of {@link HistorySubscription}s to add receivers for
     * @return The history component
     */
    public HistoryComponentImpl histComponent(@Nonnull HistorySubscription... subscriptions) {

        final Map<HistorySubscription.Topic, Optional<KafkaMessageConsumer.TopicSettings.StartFrom>> topicsAndOverrides = new HashMap<>();
        for (HistorySubscription sub : subscriptions) {
            topicsAndOverrides.put(sub.getTopic(), sub.getStartFrom());
        }

        final IMessageReceiver<VolAttachmentHistoryOuterClass.VolAttachmentHistory> historyNotificationReceiver =
                topicsAndOverrides.containsKey(HistorySubscription.Topic.HISTORY_VOL_NOTIFICATION) ?
                        historyNotificationReceiver(topicsAndOverrides.get(HistorySubscription.Topic.HISTORY_VOL_NOTIFICATION)) :
                        null;

        return new HistoryComponentImpl(
                historyNotificationReceiver,
                HistoryNotificationClientThreadPool(),
                kafkaReceiverTimeoutSeconds);
    }

    @Bean
    public Channel historyChannel() {
        return ComponentGrpcServer.newChannelBuilder(historyHost, grpcPort).build();
    }

    /**
     * Construct a channel with specified maximum message size.
     *
     * @param maxMessageSize message size
     * @return history channel
     */
    @Bean
    public Channel historyChannelWithMaxMessageSize(int maxMessageSize) {
        return ComponentGrpcServer.newChannelBuilder(historyHost, grpcPort, maxMessageSize).build();
    }

}
