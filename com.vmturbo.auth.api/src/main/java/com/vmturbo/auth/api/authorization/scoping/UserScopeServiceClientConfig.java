package com.vmturbo.auth.api.authorization.scoping;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import io.grpc.Channel;

import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc;
import com.vmturbo.common.protobuf.userscope.UserScopeServiceGrpc.UserScopeServiceBlockingStub;
import com.vmturbo.components.api.GrpcChannelFactory;

/**
 *
 */
@Configuration
@Lazy
public class UserScopeServiceClientConfig {
    @Value("${authHost}")
    private String userScopeServiceHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel userScopeServiceChannel() {
        return GrpcChannelFactory.newChannelBuilder(userScopeServiceHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean
    public UserScopeServiceBlockingStub userScopeServiceClient() {
        return UserScopeServiceGrpc.newBlockingStub(userScopeServiceChannel())
                .withInterceptors(new JwtClientInterceptor());
    }

}
