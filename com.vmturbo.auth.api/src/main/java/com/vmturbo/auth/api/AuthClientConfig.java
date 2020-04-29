package com.vmturbo.auth.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.grpc.ComponentGrpcServer;

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
        return ComponentGrpcServer.newChannelBuilder(authHost, grpcPort)
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
