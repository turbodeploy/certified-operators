package com.vmturbo.cost.api;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import io.grpc.Channel;

import com.vmturbo.components.api.GrpcChannelFactory;

/**
 * Spring configuration for a gRPC client of the Cost instance.
 * All the beans are initialized lazily.
 */
@Configuration
@Lazy
public class CostClientConfig {

    @Value("${costHost}")
    private String costHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel groupChannel() {
        return GrpcChannelFactory.newChannelBuilder(costHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }
}
