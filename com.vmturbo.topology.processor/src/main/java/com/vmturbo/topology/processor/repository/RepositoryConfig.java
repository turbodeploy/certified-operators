package com.vmturbo.topology.processor.repository;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Beans for calling the RepositoryComponent remote API.
 **/
public class RepositoryConfig {
    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public Channel repositoryChannel() {
        return GrpcChannelFactory.newChannelBuilder(repositoryHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    public RepositoryServiceGrpc.RepositoryServiceStub repositoryServiceStub() {
        return RepositoryServiceGrpc.newStub(repositoryChannel());
    }

    /**
     * Bean for Repository Client.
     *
     * @return RepositoryClient.
     */
    @Bean
    public RepositoryClient repository() {
        return new RepositoryClient(repositoryChannel(), realtimeTopologyContextId);
    }
}
