package com.vmturbo.topology.processor.plan;

import java.util.concurrent.TimeUnit;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;

@Configuration
public class PlanConfig {
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
        return GrpcChannelFactory.newChannelBuilder(planOrchestratorHost, globalConfig.grpcPort())
            .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
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

    @Bean
    public TemplateServiceBlockingStub templateServiceBlockingStub() {
        return TemplateServiceGrpc.newBlockingStub(planOrchestratorChannel());
    }

    @Bean
    public ReservationServiceBlockingStub reservationServiceBlockingStub() {
        return ReservationServiceGrpc.newBlockingStub(planOrchestratorChannel());
    }
}
