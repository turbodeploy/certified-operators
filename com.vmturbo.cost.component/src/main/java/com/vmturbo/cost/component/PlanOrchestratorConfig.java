package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.cost.component.plan.PlanService;
import com.vmturbo.cost.component.plan.PlanServiceImpl;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;

/**
 * Spring configuration file for integration with the plan orchestrator gRPC services.
 */
@Configuration
@Import({PlanOrchestratorClientConfig.class})
public class PlanOrchestratorConfig {

    /**
     * The plan orchestrator client configuration.
     */
    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    /**
     * Creates a new plan gRPC service blocking stub with the plan orchestrator's client channel.
     *
     * @return A PlanServiceBlockingStub that can be used to execute plan gRPC calls
     */
    @Bean
    public PlanServiceBlockingStub planRpcService() {
        return PlanServiceGrpc.newBlockingStub(planOrchestratorClientConfig.planOrchestratorChannel())
                .withInterceptors(new JwtClientInterceptor());
    }

    /**
     * Creates a new PlanService, which provides a high-level business abstraction on top of the plan gRPC calls.
     *
     * @param planRpcService The plan gRPC blocking stub
     * @return An implementation of the PlanService interface
     */
    @Bean
    public PlanService planService(PlanServiceBlockingStub planRpcService) {
        return new PlanServiceImpl(planRpcService);
    }
}
