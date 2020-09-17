package com.vmturbo.plan.orchestrator.project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.plan.PlanProjectREST.PlanProjectServiceController;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.plan.orchestrator.GlobalConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;
import com.vmturbo.plan.orchestrator.cpucapacity.CpuCapacityConfig;
import com.vmturbo.plan.orchestrator.market.PlanOrchestratorMarketConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Spring configuration related to plans.
 */
@Configuration
@Import({PlanOrchestratorDBConfig.class,
        GlobalConfig.class,
        RepositoryClientConfig.class,
        HistoryClientConfig.class,
        GroupClientConfig.class,
        PlanConfig.class,
        TemplatesConfig.class,
        ActionOrchestratorClientConfig.class,
        PlanOrchestratorMarketConfig.class,
        BaseKafkaProducerConfig.class})
public class PlanProjectConfig {
    @Autowired
    private PlanOrchestratorDBConfig databaseConfig;

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

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private PlanOrchestratorMarketConfig planOrchestratorMarketConfig;

    @Value("${defaultHeadroomPlanProjectJsonFile:systemPlanProjects.json}")
    private String defaultHeadroomPlanProjectJsonFile;

    @Value("${headroomCalculationForAllClusters:true}")
    private boolean headroomCalculationForAllClusters;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private CpuCapacityConfig cpuCapacityConfig;

    /**
     * Returns the external service for creating, updating, and running plans.
     *
     * @return the external service for creating, updating, and running plans.
     */
    @Bean
    public PlanProjectRpcService planProjectService() {
        return new PlanProjectRpcService(planProjectDao(), planProjectExecutor());
    }

    /**
     * Returns the instance implementing how plans are persisted and retrieved from storage.
     *
     * @return the instance implementing how plans are persisted and retrieved from storage.
     */
    @Bean
    public PlanProjectDao planProjectDao() {
        return new PlanProjectDaoImpl(databaseConfig.dsl(), globalConfig.identityInitializer());
    }

    /**
     * Returns the external service for creating, updating, and running plans.
     *
     * @return the external service for creating, updating, and running plans.
     */
    @Bean
    public PlanProjectServiceController planProjectServiceController() {
        return new PlanProjectServiceController(planProjectService());
    }

    /**
     * Gets notification sender for plan project.
     *
     * @return Plan project notification sender.
     */
    @Bean
    public PlanProjectNotificationSender planProjectNotificationSender() {
        return new PlanProjectNotificationSender(kafkaProducerConfig.kafkaMessageSender()
                        .messageSender(PlanOrchestratorClientImpl.STATUS_CHANGED_TOPIC));
    }

    /**
     * Returns the bean that tracks running plans.
     *
     * @return the bean that tracks running plans.
     */
    @Bean
    public ProjectPlanPostProcessorRegistry planProjectRuntime() {
        final ProjectPlanPostProcessorRegistry runtime = new ProjectPlanPostProcessorRegistry();
        planConfig.planDao().addStatusListener(runtime);
        planOrchestratorMarketConfig.planProjectedTopologyListener().addProjectedTopologyProcessor(runtime);
        return runtime;
    }

    /**
     * Returns the bean for executing plans.
     *
     * @return the bean for executing plans.
     */
    @Bean
    public PlanProjectExecutor planProjectExecutor() {
        return new PlanProjectExecutor(planConfig.planDao(), planProjectDao(),
                groupClientConfig.groupChannel(),
                planConfig.planService(),
                planProjectRuntime(),
                repositoryClientConfig.repositoryChannel(),
                templatesConfig.templatesDao(),
                historyClientConfig.historyChannel(),
                planProjectNotificationSender(),
                headroomCalculationForAllClusters,
                globalConfig.tpNotificationClient(),
                cpuCapacityConfig.cpuCapacityEstimator());
    }

    /**
     * Returns the bean for accessing the history service.
     *
     * @return the bean for accessing the history service.
     */
    @Bean
    public StatsHistoryServiceBlockingStub historyRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel());
    }
}
