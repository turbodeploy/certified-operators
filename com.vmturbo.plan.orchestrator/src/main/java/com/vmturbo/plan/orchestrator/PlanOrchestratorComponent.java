package com.vmturbo.plan.orchestrator;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.scheduled.ClusterRollupSchedulerConfig;

import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;

/**
 * Responsible for orchestrating plan workflow.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@Import({ScenarioConfig.class, PlanConfig.class, TemplatesConfig.class,
        DeploymentProfileConfig.class, SQLDatabaseConfig.class, ClusterRollupSchedulerConfig.class})
public class PlanOrchestratorComponent extends BaseVmtComponent {
    private static final Logger LOGGER = LogManager.getLogger();

    @Autowired
    private ScenarioConfig scenarioConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private DeploymentProfileConfig deploymentProfileConfig;

    @Value("${spring.application.name}")
    private String componentName;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Autowired
    private ClusterRollupSchedulerConfig clusterRollupSchedulerConfig;

    @PostConstruct
    private void setup() {
        LOGGER.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck("MariaDB",
                new SQLDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        dbConfig.dataSource()::getConnection));
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(PlanOrchestratorComponent.class)
                .run(args);
    }

    @Override
    public String getComponentName() {
        return componentName;
    }

    /**
     * When the Component is ready, set up the rollup schedule.
     */
    @Override
    public void onStartComponent() {
        super.onStartComponent();
        clusterRollupSchedulerConfig.clusterRollupTask().initializeSchedule();
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        return Optional.of(builder.addService(scenarioConfig.scenarioService())
                .addService(planConfig.planService())
                .addService(templatesConfig.templatesService())
                .addService(templatesConfig.templateSpecService())
                .addService(templatesConfig.discoveredTemplateDeploymentProfileService())
                .addService(deploymentProfileConfig.deploymentProfileRpcService())
                .build());
    }
}
