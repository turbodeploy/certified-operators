package com.vmturbo.plan.orchestrator.scenario;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioREST.ScenarioServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.DbAccessConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class, GlobalConfig.class, UserSessionConfig.class,
    GroupClientConfig.class, RepositoryClientConfig.class})
public class ScenarioConfig {
    @Autowired
    private DbAccessConfig databaseConfig;

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
        try {
            return new ScenarioDao(databaseConfig.dsl());

        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ScenarioDao", e);
        }
    }

    @Bean
    public ScenarioRpcService scenarioService() {
        return new ScenarioRpcService(scenarioDao(), globalConfig.identityInitializer(),
            userSessionConfig.userSessionContext(), groupServiceBlockingStub(),
                repositoryClientConfig.searchServiceClient(), supplyChainRpcService(),
                repositoryServiceBlockingStub());
    }

    @Bean
    public ScenarioServiceController scenarioServiceController() {
        return new ScenarioServiceController(scenarioService());
    }

    /**
     * Create repositoryServiceBlockingStub instance.
     *
     * @return repositoryServiceBlockingStub
     */
    @Bean
    public RepositoryServiceBlockingStub repositoryServiceBlockingStub() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    /**
     * Create JwtClientInterceptor instance.
     *
     * @return JwtClientInterceptor
     */
    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }
}
