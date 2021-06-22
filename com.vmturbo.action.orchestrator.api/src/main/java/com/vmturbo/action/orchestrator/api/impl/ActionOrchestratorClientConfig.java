package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.action.orchestrator.api.EntitySeverityClientCache;
import com.vmturbo.action.orchestrator.api.export.ActionRollupExport.ActionRollupNotification;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionApprovalRequests;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionEvent;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.common.protobuf.action.EntitySeverityNotificationOuterClass.EntitySeverityNotification;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionResponse;

/**
 * Spring configuration to import to connecto to Action Orchestrator instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Import({BaseKafkaConsumerConfig.class})
public class ActionOrchestratorClientConfig {

    /**
     * Kafka topic for action approval requests.
     */
    public static final String ACTION_APPROVAL_REQUEST_TOPIC = "external-action-approval-requests";
    /**
     * Kafka topic for sending action state updates to external backend.
     */
    public static final String ACTION_UPDATE_STATE_REQUESTS_TOPIC =
            "external-action-update-state-requests";
    /**
     * Kafka topic for sending action audit events.
     */
    public static final String ACTION_AUDIT_TOPIC = "external-action-audit-events";

    /**
     * Topic for severity update broadcasts.
     */
    public static final String SEVERITY_TOPIC = "entity-severities";

    /**
     * Topic for action stat rollup notifications.
     *
     * <p/>The action orchestrator broadcasts rolled up action data on this topic.
     */
    public static final String ROLLUP_TOPIC = "action-stat-rollups";

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConsumerConfig;

    @Value("${actionOrchestratorHost}")
    private String actionOrchestratorHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService actionOrchestratorClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orchestrator-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<ActionOrchestratorNotification> actionOrchestratorClientMessageReceiver() {
        return baseKafkaConsumerConfig.kafkaConsumer()
                .messageReceiver(ActionOrchestratorNotificationReceiver.ACTIONS_TOPIC,
                        ActionOrchestratorNotification::parseFrom);
    }

    /**
     * Action orchestrator client bean.
     *
     * @return the bean created
     */
    @Bean
    public ActionOrchestratorNotificationReceiver actionOrchestratorClient() {
        return new ActionOrchestratorNotificationReceiver(actionOrchestratorClientMessageReceiver(),
                actionOrchestratorClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }

    /**
     * Clients should use this to listen to action rollup notifications.
     *
     * @return The {@link ActionRollupNotificationReceiver}.
     */
    @Bean
    @Lazy
    public ActionRollupNotificationReceiver actionRollupNotificationReceiver() {
        return new ActionRollupNotificationReceiver(actionRollupReceiver(),
                actionOrchestratorClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }

    /**
     * An asynchronous message receiver for action approval requests.
     *
     * @return message receiver
     */
    @Bean
    @Lazy
    public IMessageReceiver<ActionApprovalRequests> createActionApprovalRequestListener() {
        return baseKafkaConsumerConfig.kafkaConsumer().messageReceiver(
                ACTION_APPROVAL_REQUEST_TOPIC, ActionApprovalRequests::parseFrom);
    }

    /**
     * An asynchronous message receiver for action rollup notifications.
     *
     * @return message receiver
     */
    @Bean
    @Lazy
    IMessageReceiver<ActionRollupNotification> actionRollupReceiver() {
        return baseKafkaConsumerConfig.kafkaConsumer().messageReceiver(
                ROLLUP_TOPIC, ActionRollupNotification::parseFrom);
    }

    /**
     * Asynchronous message receiver for entity severity updates.
     *
     * @return The {@link IMessageReceiver}.
     */
    @Bean
    @Lazy
    public IMessageReceiver<EntitySeverityNotification> entitySeverityNotificationMessageReceiver() {
        return baseKafkaConsumerConfig.kafkaConsumer().messageReceiver(
                SEVERITY_TOPIC, EntitySeverityNotification::parseFrom);
    }

    /**
     * Receiver for notifications.
     *
     * @return {@link EntitySeverityNotificationReceiver}.
     */
    @Bean
    @Lazy
    public EntitySeverityNotificationReceiver entitySeverityNotificationReceiver() {
        return new EntitySeverityNotificationReceiver(entitySeverityNotificationMessageReceiver(),
                actionOrchestratorClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }

    /**
     * An {@link EntitySeverityClientCache}, which clients can use to look up severities locally
     * instead of over gRPC.
     *
     * @return The {@link EntitySeverityClientCache}.
     */
    @Bean
    @Lazy
    public EntitySeverityClientCache entitySeverityClientCache() {
        EntitySeverityClientCache clientCache = new EntitySeverityClientCache();
        entitySeverityNotificationReceiver().addListener(clientCache);
        return clientCache;
    }

    /**
     * An asynchronous message receiver for internal action state updates.
     *
     * @return message receiver
     */
    @Bean
    @Lazy
    public IMessageReceiver<ActionResponse> createActionStateUpdateListener() {
        return baseKafkaConsumerConfig.kafkaConsumer().messageReceiver(
                ACTION_UPDATE_STATE_REQUESTS_TOPIC, ActionResponse::parseFrom);
    }

    /**
     * An asynchronous message receiver for action audit events.
     *
     * @return message receiver
     */
    @Bean
    @Lazy
    public IMessageReceiver<ActionEvent> createActionEventsListener() {
        return baseKafkaConsumerConfig.kafkaConsumer().messageReceiver(ACTION_AUDIT_TOPIC,
                ActionEvent::parseFrom);
    }

    /**
     * Action orchestrator gRPC channel.
     *
     * @return gRPC channel.
     */
    @Bean
    public Channel actionOrchestratorChannel() {
        return ComponentGrpcServer.newChannelBuilder(actionOrchestratorHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }
}
