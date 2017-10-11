package com.vmturbo.topology.processor.api.impl;

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
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
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

    @Value("${server.port}")
    private int topologyProcessorPort;

    @Value("${server.grpcPort}")
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
    protected IMessageReceiver<Topology> topologyBroadcastReceiver() {
        return baseKafkaConfig.kafkaConsumer()
                .messageReceiver(TopologyProcessorClient.TOPOLOGY_BROADCAST_TOPIC,
                        Topology::parseFrom);
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return TopologyProcessorClient.rpcAndNotification(topologyProcessorClientConnectionConfig(),
                topologyProcessorClientThreadPool(), topologyNotificationReceiver(),
                topologyBroadcastReceiver());
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
        return PingingChannelBuilder.forAddress(topologyProcessorHost, topologyProcessorRpcPort)
                .usePlaintext(true)
                .build();
    }
}
