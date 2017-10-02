package com.vmturbo.topology.processor.templates;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceBlockingStub;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;

@Configuration
public class DiscoveredTemplateDeploymentProfileConfig {
    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Value("${planOrchestratorHost}")
    private String planOrchestratorHost;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public Channel planOrchestratorChannel() {
        return PingingChannelBuilder.forAddress(planOrchestratorHost, globalConfig.grpcPort())
            .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
            .usePlaintext(true)
            .build();
    }

    @Bean
    public DiscoveredTemplateDeploymentProfileServiceBlockingStub templateDeploymentProfileRpcService() {
        return DiscoveredTemplateDeploymentProfileServiceGrpc.newBlockingStub(planOrchestratorChannel());
    }

    @Bean
    public DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader() {
        return new DiscoveredTemplateDeploymentProfileUploader(entityConfig.entityStore(),
                                                               templateDeploymentProfileRpcService());
    }
}
