package com.vmturbo.topology.processor.api.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Spring configuration to import to connect to TopologyProcessor instance.
 * All the beans are initialized lazily, so some types of connections can be not started up by
 * default.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class})
public class TopologyProcessorClientConfig {

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConfig;

    @Value("${topologyProcessorHost}")
    private String topologyProcessorHost;

    @Value("${serverHttpPort}")
    private int topologyProcessorPort;

    @Value("${topologyProcessorRoute:}")
    private String topologyProcessorRoute;

    @Value("${serverGrpcPort}")
    private int topologyProcessorRpcPort;

    @Value("${restClientConnectionRequestTimeoutMins:60}")
    private int restClientConnectionRequestTimeoutMins;

    @Value("${restClientConnectTimeoutMins:60}")
    private int restClientConnectTimeoutMins;

    @Value("${restClientReadTimeoutMins:60}")
    private int restClientReadTimeoutMins;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService topologyProcessorClientThreadPool() {
        return Executors.newCachedThreadPool(topologyProcessorClientThreadFactory());
    }

    @Bean
    protected ThreadFactory topologyProcessorClientThreadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("tp-api-%d").build();
    }

    @Bean
    protected ComponentApiConnectionConfig topologyProcessorClientConnectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(topologyProcessorHost, topologyProcessorPort, topologyProcessorRoute)
                .setTimeOuts(restClientConnectionRequestTimeoutMins, restClientConnectTimeoutMins,
                    restClientReadTimeoutMins)
                .build();
    }

    protected IMessageReceiver<TopologyProcessorNotification> topologyNotificationReceiver(
            Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                    new TopicSettings(TopologyProcessorClient.NOTIFICATIONS_TOPIC, startFrom),
                    TopologyProcessorNotification::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.NOTIFICATIONS_TOPIC,
                    TopologyProcessorNotification::parseFrom));
    }

    protected IMessageReceiver<Topology> liveTopologyBroadcastReceiver(
            @Nonnull final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                    new TopicSettings(TopologyProcessorClient.TOPOLOGY_LIVE, startFrom),
                    Topology::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.TOPOLOGY_LIVE,
                    Topology::parseFrom));
    }

    protected IMessageReceiver<Topology> planTopologyBroadcastReceiver(
            @Nonnull final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer()
                .messageReceiversWithSettings(Arrays.asList(
                    new TopicSettings(TopologyProcessorClient.TOPOLOGY_USER_PLAN, startFrom),
                    new TopicSettings(TopologyProcessorClient.TOPOLOGY_SCHEDULED_PLAN, startFrom)),
                    Topology::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer()
                .messageReceiver(Arrays.asList(TopologyProcessorClient.TOPOLOGY_USER_PLAN,
                    TopologyProcessorClient.TOPOLOGY_SCHEDULED_PLAN), Topology::parseFrom));
    }

    protected IMessageReceiver<TopologySummary> topologySummaryReceiver(
            @Nonnull final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                    new TopicSettings(TopologyProcessorClient.TOPOLOGY_SUMMARIES, startFrom),
                    TopologySummary::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                    // The default is to start from the beginning.
                    new TopicSettings(TopologyProcessorClient.TOPOLOGY_SUMMARIES, StartFrom.BEGINNING),
                    TopologySummary::parseFrom));
    }

    protected IMessageReceiver<EntitiesWithNewState> entitiesWithNewStateReceiver(
        @Nonnull final Optional<StartFrom> startFromOverride) {
        return startFromOverride
            .map(startFrom -> baseKafkaConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                    new TopicSettings(TopologyProcessorClient.ENTITIES_WITH_NEW_STATE, startFrom),
                    EntitiesWithNewState::parseFrom))
            .orElseGet(() -> baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.ENTITIES_WITH_NEW_STATE,
                    EntitiesWithNewState::parseFrom));
    }

    /**
     * This is a lazy bean prototype. It will subscribe on different topics based on the input
     * parameters. Everithing will be later collected by Spring to destroy automatically.
     *
     * @param subscriptions set of features to subscribe to
     * @return topology processor client implementation
     */
    public TopologyProcessor topologyProcessor(@Nonnull final TopologyProcessorSubscription... subscriptions) {
        final Map<Topic, TopologyProcessorSubscription> subscriptionsByTopic = new HashMap<>();
        for (TopologyProcessorSubscription sub : subscriptions) {
            subscriptionsByTopic.put(sub.getTopic(), sub);
        }

        final IMessageReceiver<TopologyProcessorNotification> notificationsReceiver
            = subscriptionsByTopic.containsKey(Topic.Notifications)
            ? topologyNotificationReceiver(subscriptionsByTopic.get(Topic.Notifications).getStartFrom())
            : null;
        final IMessageReceiver<Topology> liveReceiver
            = subscriptionsByTopic.containsKey(Topic.LiveTopologies)
            ? liveTopologyBroadcastReceiver(subscriptionsByTopic.get(Topic.LiveTopologies).getStartFrom())
            : null;
        final IMessageReceiver<Topology> planReceiver
            = subscriptionsByTopic.containsKey(Topic.PlanTopologies)
            ? planTopologyBroadcastReceiver(subscriptionsByTopic.get(Topic.PlanTopologies).getStartFrom())
            : null;
        final IMessageReceiver<TopologySummary> summaryReceiver
            = subscriptionsByTopic.containsKey(Topic.TopologySummaries)
            ? topologySummaryReceiver(subscriptionsByTopic.get(Topic.TopologySummaries).getStartFrom())
            : null;
        final IMessageReceiver<EntitiesWithNewState> entitiesWithNewStateReceiver
            = subscriptionsByTopic.containsKey(Topic.EntitiesWithNewState)
            ? entitiesWithNewStateReceiver(subscriptionsByTopic.get(Topic.EntitiesWithNewState).getStartFrom())
            : null;
        return TopologyProcessorClient.rpcAndNotification(topologyProcessorClientConnectionConfig(),
            topologyProcessorClientThreadPool(), notificationsReceiver, liveReceiver,
            planReceiver, summaryReceiver, entitiesWithNewStateReceiver);
    }

    public TopologyProcessor topologyProcessorRpcOnly() {
        return TopologyProcessorClient.rpcOnly(topologyProcessorClientConnectionConfig());
    }

    /**
     * The gRPC channel to the Topology Processor.
     * This connection can - and should - be shared by all stubs making calls
     * to the Topology Processor's services.
     *
     * @return The gRPC channel.
     */
    @Bean
    public Channel topologyProcessorChannel() {
        return ComponentGrpcServer.newChannelBuilder(topologyProcessorHost, topologyProcessorRpcPort)
                .build();
    }

    /**
     * Creates action states message receiver which will be able to receive action state changes
     * reported from external action approval backend.
     *
     * @return the bean created
     */
    @Nonnull
    public IMessageReceiver<GetActionStateResponse> createActionStateReceiver() {
        return baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.EXTERNAL_ACTION_UPDATES_TOPIC,
                        GetActionStateResponse::parseFrom);
    }

    /**
     * Returns the Kafka receiver for ActionApprovalResponse messages sent when the probe completes
     * sending the external approvals to an external system like ServiceNOW.
     *
     * @return the kafka receiver for ActionApprovalResponse external approvals sent.
     */
    @Nonnull
    public IMessageReceiver<ActionApprovalResponse> createActionApprovalResponseReceiver() {
        return baseKafkaConfig.kafkaConsumer()
            .messageReceiver(TopologyProcessorClient.EXTERNAL_ACTION_APPROVAL_RESPONSE,
                ActionApprovalResponse::parseFrom);
    }
}
