package com.vmturbo.plan.orchestrator.project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTOREST.PlanProjectServiceController;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class,
        GlobalConfig.class,
        RepositoryClientConfig.class,
        HistoryClientConfig.class,
        GroupClientConfig.class,
        PlanConfig.class,
        TemplatesConfig.class})
public class PlanProjectConfig {
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private TemplatesConfig templatesConfig;

    @Value("${defaultHeadroomPlanProjectJsonFile:systemPlanProjects.json}")
    private String defaultHeadroomPlanProjectJsonFile;

    @Bean
    public PlanProjectRpcService planProjectService() {
        return new PlanProjectRpcService(planProjectDao(), planProjectExecutor());
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

    @Bean
    public PlanProjectExecutor planProjectExecutor() {
        return new PlanProjectExecutor(planConfig.planDao(), groupRpcService(),
                planConfig.planService(), planProjectRuntime(),
                repositoryClientConfig.repositoryChannel(),
                templatesConfig.templatesDao(),
                historyClientConfig.historyChannel());
    }
    @Bean
    public GroupServiceGrpc.GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }
}
