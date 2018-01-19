package com.vmturbo.topology.processor.repository;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Beans for calling the RepositoryComponent remote API.
 **/
public class RepositoryConfig {
    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel repositoryChannel() {
        return PingingChannelBuilder.forAddress(repositoryHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public RepositoryServiceGrpc.RepositoryServiceStub repositoryServiceStub() {
        return RepositoryServiceGrpc.newStub(repositoryChannel());
    }

    @Bean
    public RepositoryClient repository() {
        return new RepositoryClient(repositoryChannel());
    }
}
