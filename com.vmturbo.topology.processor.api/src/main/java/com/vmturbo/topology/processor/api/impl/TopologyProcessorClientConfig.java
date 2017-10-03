package com.vmturbo.topology.processor.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

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
public class TopologyProcessorClientConfig {

    @Value("${topologyProcessorHost}")
    private String topologyProcessorHost;

    @Value("${server.port}")
    private int topologyProcessorPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Value("${server.grpcPort}")
    private int topologyProcessorRpcPort;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        return Executors.newCachedThreadPool(threadFactory());
    }

    @Bean
    protected ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("tp-api-%d").build();
    }

    @Bean
    protected ComponentApiConnectionConfig connectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(topologyProcessorHost, topologyProcessorPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean
    protected IMessageReceiver<TopologyProcessorNotification> messageReceiver() {
        return new TopologyProcessorMessageReceiver(connectionConfig(), threadPool());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return TopologyProcessorClient.rpcAndNotification(connectionConfig(), threadPool(),
                messageReceiver());
    }

    @Bean
    public TopologyProcessor topologyProcessorRpcOnly() {
        return TopologyProcessorClient.rpcOnly(connectionConfig());
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
