package com.vmturbo.plan.orchestrator.project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.PlanDTOREST.PlanProjectServiceController;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class,
        GlobalConfig.class,
        RepositoryClientConfig.class,
        PlanConfig.class})
public class PlanProjectConfig {
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private PlanConfig planConfig;

    @Bean
    public PlanProjectRpcService planProjectService() {
        return new PlanProjectRpcService(planProjectDao());
    }

    @Bean
    public PlanProjectDao planProjectDao() {
        return new PlanProjectDaoImpl(databaseConfig.dsl(), globalConfig.identityInitializer());
    }

    @Bean
    public PlanProjectServiceController planProjectServiceController() {
        return new PlanProjectServiceController(planProjectService());
    }

    @Bean
    public ProjectPlanPostProcessorRegistry planProjectRuntime() {
        final ProjectPlanPostProcessorRegistry runtime = new ProjectPlanPostProcessorRegistry();
        planConfig.planDao().addStatusListener(runtime);
        return runtime;
    }
}
