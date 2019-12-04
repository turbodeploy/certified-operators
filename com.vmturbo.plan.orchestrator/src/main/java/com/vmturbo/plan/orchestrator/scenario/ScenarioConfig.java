package com.vmturbo.plan.orchestrator.scenario;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTOREST.ScenarioServiceController;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, GlobalConfig.class, UserSessionConfig.class,
    GroupClientConfig.class, RepositoryClientConfig.class})
public class ScenarioConfig {
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Bean
    public ScenarioDao scenarioDao() {
        return new ScenarioDao(databaseConfig.dsl());
    }

    @Bean
    public ScenarioRpcService scenarioService() {
        return new ScenarioRpcService(scenarioDao(), globalConfig.identityInitializer(),
            userSessionConfig.userSessionContext(), groupServiceBlockingStub(),
            repositoryClientConfig.searchServiceClient());
    }

    @Bean
    public ScenarioServiceController scenarioServiceController() {
        return new ScenarioServiceController(scenarioService());
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())
                .withInterceptors(jwtClientInterceptor());
    }

}
