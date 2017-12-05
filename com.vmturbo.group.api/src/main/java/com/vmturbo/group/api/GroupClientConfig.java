package com.vmturbo.group.api;

import java.util.concurrent.TimeUnit;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.grpc.extensions.PingingChannelBuilder;

@Configuration
@Lazy
public class GroupClientConfig {

    @Value("${groupHost}")
    private String groupHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel groupChannel() {
        return PingingChannelBuilder.forAddress(groupHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }
}
