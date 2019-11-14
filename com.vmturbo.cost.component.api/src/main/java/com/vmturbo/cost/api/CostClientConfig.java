package com.vmturbo.cost.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.cost.api.impl.CostComponentImpl;
import com.vmturbo.cost.api.impl.CostSubscription;
import com.vmturbo.cost.api.impl.CostSubscription.Topic;

/**
 * Spring configuration for a gRPC client of the Cost instance.
 * All the beans are initialized lazily.
 */
@Configuration
@Import(BaseKafkaConsumerConfig.class)
@Lazy
public class CostClientConfig {

    @Value("${costHost}")
    private String costHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService costNotificationClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("cost-client-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public Channel costChannel() {
        return GrpcChannelFactory.newChannelBuilder(costHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    protected IMessageReceiver<CostNotification> costNotificationReceiver(
            @Nonnull final Optional<StartFrom> startFromOverride) {
        return startFromOverride
                .map(startFrom -> baseKafkaConfig.kafkaConsumer().messageReceiverWithSettings(
                        new TopicSettings(CostComponentImpl.COST_NOTIFICATIONS, startFrom),
                        CostNotification::parseFrom))
                .orElseGet(() -> baseKafkaConfig.kafkaConsumer().messageReceiver(
                        CostComponentImpl.COST_NOTIFICATIONS,
                        CostNotification::parseFrom));
    }

    /**
     * The returns the cost component for adding listeners.
     *
     * @param subscriptions The set of {@link CostSubscription}s to add receivers for
     * @return The cost component
     */
    public CostComponentImpl costComponent(@Nonnull CostSubscription... subscriptions) {

        final Map<Topic, Optional<StartFrom>> topicsAndOverrides = new HashMap<>();
        for (CostSubscription sub : subscriptions) {
            topicsAndOverrides.put(sub.getTopic(), sub.getStartFrom());
        }

        final IMessageReceiver<CostNotification> costNotificationReceiver =
                topicsAndOverrides.containsKey(Topic.COST_STATUS_NOTIFICATION) ?
                        costNotificationReceiver(topicsAndOverrides.get(Topic.COST_STATUS_NOTIFICATION)) :
                        null;

        return new CostComponentImpl(
                costNotificationReceiver,
                costNotificationClientThreadPool(),
                kafkaReceiverTimeoutSeconds);
    }

}
