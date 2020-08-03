package com.vmturbo.repository.api.impl;

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

import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.TopologyAvailabilityTracker;

/**
 * Configuration for the Repository gRPC API Client.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class})
public class RepositoryClientConfig {

    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${serverHttpPort}")
    private int repositoryPort;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService repositoryClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("repository-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<RepositoryNotification> repositoryClientMessageReceiver() {
        return kafkaConsumerConfig.kafkaConsumer().messageReceiverWithSettings(
            new TopicSettings(RepositoryNotificationReceiver.TOPOLOGY_TOPIC, StartFrom.BEGINNING),
            RepositoryNotification::parseFrom);
    }

    @Bean
    public RepositoryNotificationReceiver repository() {
        return new RepositoryNotificationReceiver(repositoryClientMessageReceiver(),
                repositoryClientThreadPool(), kafkaReceiverTimeoutSeconds);
    }

    @Bean
    public Channel repositoryChannel() {
        return ComponentGrpcServer.newChannelBuilder(repositoryHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    /**
     * Utility class to help wait for topologies to become available.
     *
     * @return The {@link TopologyAvailabilityTracker}.
     */
    @Bean
    public TopologyAvailabilityTracker topologyAvailabilityTracker() {
        final TopologyAvailabilityTracker topologyAvailabilityTracker =
            new TopologyAvailabilityTracker(realtimeTopologyContextId);
        repository().addListener(topologyAvailabilityTracker);
        return topologyAvailabilityTracker;
    }

    /**
     * Bean for Repository Client.
     *
     * @return RepositoryClient.
     */
    @Bean
    public RepositoryClient repositoryClient() {
        return new RepositoryClient(repositoryChannel(), realtimeTopologyContextId);
    }

    @Bean
    public SearchServiceBlockingStub searchServiceClient() {
        return SearchServiceGrpc.newBlockingStub(repositoryChannel());
    }

    @Nonnull
    public String getRepositoryHost() {
        return repositoryHost;
    }

    public int getRepositoryPort() {
        return repositoryPort;
    }
}
