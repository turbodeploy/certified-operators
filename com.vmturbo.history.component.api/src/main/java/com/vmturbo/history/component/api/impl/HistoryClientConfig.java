package com.vmturbo.history.component.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;

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
