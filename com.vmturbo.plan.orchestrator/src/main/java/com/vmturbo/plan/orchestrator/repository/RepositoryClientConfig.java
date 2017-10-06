package com.vmturbo.plan.orchestrator.repository;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.grpc.Channel;

import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.repository.api.RepositoryClient;

@Configuration
public class RepositoryClientConfig {

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
    public RepositoryClient repositoryClient() {
        return new RepositoryClient(repositoryChannel());
    }
}
