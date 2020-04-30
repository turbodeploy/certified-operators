package com.vmturbo.clustermgr.api;

import java.util.concurrent.TimeUnit;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;

/**
 * Configuration cluster manager clients can use to connect to cluster manager.
 */
@Configuration
@Lazy
public class ClusterMgrClientConfig {
    @Value("${clusterMgrHost}")
    private String clusterMgrHost;

    @Value("${clusterMgrRoute:}")
    private String clusterMgrRoute;

    @Value("${serverHttpPort}")
    private int clusterMgrPort;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${kafkaReceiverTimeoutSeconds:3600}")
    private int kafkaReceiverTimeoutSeconds;

    /**
     * A persistent channel to make gRPC calls to the cluster manager.
     *
     * @return The {@link Channel}.
     */
    @Bean
    public Channel clustermgrChannel() {
        return ComponentGrpcServer.newChannelBuilder(clusterMgrHost, grpcPort)
            .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
            .build();
    }

    /**
     * REST client to issue REST commands to cluster manager.
     *
     * @return The {@link ClusterMgrRestClient}.
     */
    @Bean
    public ClusterMgrRestClient restClient() {
        return ClusterMgrClient.createClient(ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(clusterMgrHost, clusterMgrPort, clusterMgrRoute)
                .build());
    }
}
