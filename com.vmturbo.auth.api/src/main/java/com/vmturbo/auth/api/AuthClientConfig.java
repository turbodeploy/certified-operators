package com.vmturbo.auth.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
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

    @Value("${clientServiceHost:" + SecurityConstant.HYDRA_ADMIN + "}")
    private String clientServiceHost;

    @Value("${clientServicePort:" + SecurityConstant.HYDRA_ADMIN_PORT + "}")
    private Integer clientServicePort;

    @Value("${clientServiceScheme:" + SecurityConstant.HTTP + "}")
    private String clientServiceScheme;

    @Value("${clientServicePath:" + SecurityConstant.HYDRA_CLIENTS_PATH + "}")
    private String clientServicePath;

    @Value("${clientNetworkHost:" + SecurityConstant.CLIENT_NETWORK + "}")
    private String clientNetworkHost;

    @Value("${clientNetworkPort:" + SecurityConstant.CLIENT_NETWORK_PORT + "}")
    private Integer clientNetworkPort;

    @Value("${clientNetworkScheme:" + SecurityConstant.HTTP + "}")
    private String clientNetworkScheme;

    @Value("${clientNetworkPath:" + SecurityConstant.CLIENT_NETWORK_PATH + "}")
    private String clientNetworkPath;

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

    public String getClientServiceHost() {
        return clientServiceHost;
    }

    public Integer getClientServicePort() {
        return clientServicePort;
    }

    public String getClientServiceScheme() {
        return clientServiceScheme;
    }

    public String getClientServicePath() {
        return clientServicePath;
    }

    public String getClientNetworkHost() {
        return clientNetworkHost;
    }

    public Integer getClientNetworkPort() {
        return clientNetworkPort;
    }

    public String getClientNetworkScheme() {
        return clientNetworkScheme;
    }

    public String getClientNetworkPath() {
        return clientNetworkPath;
    }
}
