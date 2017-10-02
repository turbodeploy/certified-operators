package com.vmturbo.group;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;

@Configuration
public class GrpcConfig {

    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel searchChannel() {
        return PingingChannelBuilder.forAddress(repositoryHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public SearchServiceGrpc.SearchServiceStub searchServiceStub() {
        return SearchServiceGrpc.newStub(searchChannel());
    }

    @Bean
    public SearchServiceGrpc.SearchServiceBlockingStub searchServiceBlockingStub() {
        return SearchServiceGrpc.newBlockingStub(searchChannel());
    }
}
