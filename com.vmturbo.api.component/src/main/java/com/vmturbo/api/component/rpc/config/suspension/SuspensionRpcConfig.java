package com.vmturbo.api.component.config.suspension;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.grpc.ComponentGrpcServer;

/**
 * Spring configuration that sets up suspension related grpc service configuration.
 */
@Configuration
public class SuspensionRpcConfig {

    @Value("${suspendHost}")
    private String host;

    @Value("${suspendGrpcPort}")
    private int grpcPort;

    /**
     * The service for message conversion.
     *
     * @return the service for message conversion.
     */
    @Bean
    public Channel suspensionChannel() {
        return ComponentGrpcServer.newChannelBuilder(host, grpcPort)
                .build();
    }
}
