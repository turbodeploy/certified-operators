package com.vmturbo.plan.orchestrator.deployment.profile;


import com.vmturbo.common.protobuf.plan.DeploymentProfileDTOREST.DeploymentProfileServiceController;
import com.vmturbo.plan.orchestrator.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.sql.SQLException;

@Configuration
@Import(DbAccessConfig.class)
public class DeploymentProfileConfig {

    @Autowired
    private DbAccessConfig databaseConfig;

    @Bean
    public DeploymentProfileDaoImpl deploymentProfileDao() {
        try {
            return new DeploymentProfileDaoImpl(databaseConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create DeploymentProfileDao", e);
        }
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
