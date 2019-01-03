package com.vmturbo.group.api;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import io.grpc.Channel;

import com.vmturbo.components.api.GrpcChannelFactory;

@Configuration
@Lazy
public class GroupClientConfig {

    /**
     * The max message size.
     *
     * The call to get entity settings can return 10MB of data (or more, if we add more settings)
     * in a 200k setting topology, so places where we need entity settings for a lot of entities
     * will fail to deserialize with the default message size (4MB).
     *
     * In the long term, we should reduce the size of that message (OM-32762).
     */
    public static final int MAX_MSG_SIZE_BYTES = 40 * 1024 * 1024;

    @Value("${groupHost}")
    private String groupHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel groupChannel() {
        return GrpcChannelFactory.newChannelBuilder(groupHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                // TODO (roman, Mar 8 2018) OM-32762: Go back to default max message size once the
                // call to get entity settings is optimized.
                .maxInboundMessageSize(MAX_MSG_SIZE_BYTES)
                .build();
    }
}
