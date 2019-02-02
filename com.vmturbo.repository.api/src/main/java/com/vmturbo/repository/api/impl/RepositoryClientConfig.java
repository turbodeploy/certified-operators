package com.vmturbo.repository.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.RepositoryClient;

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
        return kafkaConsumerConfig.kafkaConsumer()
                .messageReceiver(RepositoryNotificationReceiver.TOPOLOGY_TOPIC,
                        RepositoryNotification::parseFrom);
    }

    @Bean
    public Repository repository() {
        return new RepositoryNotificationReceiver(repositoryClientMessageReceiver(),
                repositoryClientThreadPool());
    }

    @Bean
    public Channel repositoryChannel() {
        return GrpcChannelFactory.newChannelBuilder(repositoryHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    public RepositoryClient repositoryClient() {
        return new RepositoryClient(repositoryChannel());
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
