package com.vmturbo.plan.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;

/**
 * Spring configuration to import to connect to Action Orchestrator instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class})
public class PlanOrchestratorClientConfig {

    @Value("${planOrchestratorHost}")
    private String planOrchestratorHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Autowired
    private BaseKafkaConsumerConfig consumerConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService planOrchestratorClientThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "plan-orchestrator-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<PlanInstance> planInstanceReceiver() {
        return consumerConfig.kafkaConsumer()
                .messageReceiver(PlanOrchestratorClientImpl.STATUS_CHANGED_TOPIC,
                        PlanInstance::parseFrom);
    }

    /**
     * Create a message receiver for {@link ReservationChanges}.
     *
     * @return the message receiver for reservations.
     */
    @Bean
    protected IMessageReceiver<ReservationChanges> reservationChangesMessageReceiver() {
        return consumerConfig.kafkaConsumer()
            .messageReceiver(PlanOrchestratorClientImpl.RESERVATION_NOTIFICATION_TOPIC,
                ReservationChanges::parseFrom);
    }

    @Bean
    public PlanOrchestrator planOrchestrator() {
        return new PlanOrchestratorClientImpl(planInstanceReceiver(),
            reservationChangesMessageReceiver(),
            planOrchestratorClientThreadPool(),
            kafkaReceiverTimeoutSeconds);
    }

    @Bean
    public Channel planOrchestratorChannel() {
        return GrpcChannelFactory.newChannelBuilder(planOrchestratorHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }
}
