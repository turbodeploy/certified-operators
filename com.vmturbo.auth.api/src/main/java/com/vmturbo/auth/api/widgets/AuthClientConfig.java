package com.vmturbo.auth.api.widgets;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.grpc.Channel;

import com.vmturbo.components.api.GrpcChannelFactory;

/**
 * Spring configuration for a GRPC client of the Widgets functionality
 **/
@Configuration
public class AuthClientConfig {

    @Value("${authHost}")
    private String authHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel authClientChannel() {
        return GrpcChannelFactory.newChannelBuilder(authHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

}
