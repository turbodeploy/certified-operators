package com.vmturbo.auth.component;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

/**
 * Configuration of repository gRPC and kafka notification clients for use by the auth component
 * services.
 */

@Configuration
@Lazy
@Import(BaseKafkaConsumerConfig.class)
public class RepositoryClientConfig {
    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;

    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${serverHttpPort}")
    private int repositoryPort;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    @Bean
    public Channel repositoryChannel() {
        return GrpcChannelFactory.newChannelBuilder(repositoryHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    public SearchServiceBlockingStub searchServiceClient() {
        return SearchServiceGrpc.newBlockingStub(repositoryChannel());
    }

    @Bean
    protected IMessageReceiver<RepositoryNotification> repositoryClientMessageReceiver() {
        return kafkaConsumerConfig.kafkaConsumer()
                .messageReceiver(RepositoryNotificationReceiver.TOPOLOGY_TOPIC,
                        RepositoryNotification::parseFrom);
    }

    @Bean
    public RepositoryNotificationReceiver repositoryListener() {
        return new RepositoryNotificationReceiver(repositoryClientMessageReceiver(),
                Executors.newCachedThreadPool(), kafkaReceiverTimeoutSeconds);
    }

}
