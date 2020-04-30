package com.vmturbo.plan.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanStatusNotification;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector.PlanGarbageCollector;

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

    @Value("${realtimeTopologyContextId:7777777}")
    private long realtimeTopologyContextId;

    /**
     * The interval at which we try to purge data for plans that got deleted in the plan
     * orchestrator, but did not get properly deleted in the component that has some
     * plan-related data.
     */
    @Value("${planGarbageCollectionIntervalSeconds:14400}")
    private long planGarbageCollectionIntervalSeconds;

    @Autowired
    private BaseKafkaConsumerConfig consumerConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService planOrchestratorClientThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "plan-orchestrator-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<PlanStatusNotification> planInstanceReceiver() {
        return consumerConfig.kafkaConsumer()
                .messageReceiver(PlanOrchestratorClientImpl.STATUS_CHANGED_TOPIC,
                        PlanStatusNotification::parseFrom);
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
        return ComponentGrpcServer.newChannelBuilder(planOrchestratorHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    /**
     * Cleanup thread for stale plan data.
     *
     * @return The {@link ScheduledExecutorService}.
     */
    @Bean
    public ScheduledExecutorService costPlanGarbageExecutorService() {
        return Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("plan-garbage-%d")
            .build());
    }

    /**
     * Create a new {@link PlanGarbageDetector} for use in the calling spring context.
     *
     * @param planGarbageCollector The {@link PlanGarbageCollector} implementation, used to retrieve
     *                             and delete stale plan-related data in the component that's using
     *                             the {@link PlanGarbageDetector}.
     * @return The {@link PlanGarbageDetector}, which will asynchronously clean up expired plans.
     */
    @Nonnull
    public PlanGarbageDetector newPlanGarbageDetector(@Nonnull final PlanGarbageCollector planGarbageCollector) {
        final PlanGarbageDetector detector = new PlanGarbageDetector(PlanServiceGrpc.newBlockingStub(planOrchestratorChannel()),
            planGarbageCollectionIntervalSeconds,
            TimeUnit.SECONDS,
            costPlanGarbageExecutorService(),
            planGarbageCollector,
            realtimeTopologyContextId);
        planOrchestrator().addPlanListener(detector);
        return detector;
    }
}
