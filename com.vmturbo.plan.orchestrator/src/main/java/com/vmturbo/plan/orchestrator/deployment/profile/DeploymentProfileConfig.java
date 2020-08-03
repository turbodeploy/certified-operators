package com.vmturbo.plan.orchestrator.deployment.profile;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOREST.DeploymentProfileServiceController;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;

@Configuration
@Import(PlanOrchestratorDBConfig.class)
public class DeploymentProfileConfig {

    @Autowired
    private PlanOrchestratorDBConfig databaseConfig;

    @Bean
    public DeploymentProfileDaoImpl deploymentProfileDao() {
        return new DeploymentProfileDaoImpl(databaseConfig.dsl());
    }

    @Bean
    public DeploymentProfileRpcService deploymentProfileRpcService() {
        return new DeploymentProfileRpcService(deploymentProfileDao());
    }

    @Bean
    public DeploymentProfileServiceController deploymentProfileServiceController() {
        return new DeploymentProfileServiceController(deploymentProfileRpcService());
    }
}
