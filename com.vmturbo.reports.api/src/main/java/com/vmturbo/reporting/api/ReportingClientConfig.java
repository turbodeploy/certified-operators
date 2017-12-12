package com.vmturbo.reporting.api;

import java.util.concurrent.TimeUnit;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.grpc.extensions.PingingChannelBuilder;

/**
 * Client configuration to connect to reporting component.
 */
@Configuration
public class ReportingClientConfig {

    @Value("${reportingHost}")
    private String reportingHost;
    @Value("${server.grpcPort}")
    private int grpcPort;
    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    /**
     * GRPC channel to connect to reporting component's GRPC services.
     *
     * @return GRPC channel bean
     */
    @Bean
    public Channel reportingChannel() {
        return PingingChannelBuilder.forAddress(reportingHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }
}
