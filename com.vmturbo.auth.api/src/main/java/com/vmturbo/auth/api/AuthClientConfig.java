package com.vmturbo.auth.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

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

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort}")
    private Integer authPort;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel authClientChannel() {
        return GrpcChannelFactory.newChannelBuilder(authHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Nonnull
    public String getAuthHost() {
        return authHost;
    }

    @Nonnull
    public String getAuthRoute() {
        return authRoute;
    }

    @Nonnull
    public Integer getAuthPort() {
        return authPort;
    }
}
