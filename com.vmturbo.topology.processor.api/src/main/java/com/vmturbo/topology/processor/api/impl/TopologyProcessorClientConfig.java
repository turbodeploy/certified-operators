package com.vmturbo.topology.processor.api.impl;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Channel;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;

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

    @Value("${serverGrpcPort}")
    private int topologyProcessorRpcPort;

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
                .setHostAndPort(topologyProcessorHost, topologyProcessorPort)
                .build();
    }

    @Bean
    protected IMessageReceiver<TopologyProcessorNotification> topologyNotificationReceiver() {
        return baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.NOTIFICATIONS_TOPIC,
                        TopologyProcessorNotification::parseFrom);
    }

    @Bean
    protected IMessageReceiver<Topology> liveTopologyBroadcastReceiver() {
        return baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.TOPOLOGY_LIVE,
                        Topology::parseFrom);
    }

    @Bean
    protected IMessageReceiver<Topology> planTopologyBroadcastReceiver() {
        return baseKafkaConfig.kafkaConsumer()
                .messageReceiver(Arrays.asList(TopologyProcessorClient.TOPOLOGY_USER_PLAN,
                        TopologyProcessorClient.TOPOLOGY_SCHEDULED_PLAN), Topology::parseFrom);
    }

    @Bean
    protected IMessageReceiver<TopologySummary> topologySummaryReceiver() {
        return baseKafkaConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                        new TopicSettings(TopologyProcessorClient.TOPOLOGY_SUMMARIES, StartFrom.BEGINNING),
                        TopologySummary::parseFrom);
    }

    /**
     * This is a lazy bean prototype. It will subscribe on different topics based on the input
     * parameters. Everithing will be later collected by Spring to destroy automatically.
     *
     * @param subscriptions set of features to subscribe to
     * @return topology processor client implementation
     */
    public TopologyProcessor topologyProcessor(final Set<Subscription> subscriptions) {
        final IMessageReceiver<TopologyProcessorNotification> notificationsReceiver
                = subscriptions.contains(Subscription.Notifications)
                    ? topologyNotificationReceiver()
                    : null;
        final IMessageReceiver<Topology> liveReceiver
                = subscriptions.contains(Subscription.LiveTopologies)
                    ? liveTopologyBroadcastReceiver()
                    : null;
        final IMessageReceiver<Topology> planReceiver
                = subscriptions.contains(Subscription.PlanTopologies)
                    ? planTopologyBroadcastReceiver()
                    : null;
        final IMessageReceiver<TopologySummary> summaryReceiver
                = subscriptions.contains(Subscription.TopologySummaries)
                    ? topologySummaryReceiver()
                    : null;
        return TopologyProcessorClient.rpcAndNotification(topologyProcessorClientConnectionConfig(),
                topologyProcessorClientThreadPool(), notificationsReceiver, liveReceiver,
                planReceiver, summaryReceiver);
    }

    @Bean
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
        return GrpcChannelFactory.newChannelBuilder(topologyProcessorHost, topologyProcessorRpcPort)
                .build();
    }

    public enum Subscription {
        Notifications, LiveTopologies, PlanTopologies, TopologySummaries;
    }
}
