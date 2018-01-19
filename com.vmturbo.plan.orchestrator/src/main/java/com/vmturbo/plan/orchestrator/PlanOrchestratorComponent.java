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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.plan.orchestrator.reservation.ReservationConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.scheduled.ClusterRollupSchedulerConfig;
import com.vmturbo.plan.orchestrator.scheduled.PlanDeletionSchedulerConfig;
import com.vmturbo.plan.orchestrator.scheduled.PlanProjectSchedulerConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Responsible for orchestrating plan workflow.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@Import({DeploymentProfileConfig.class,
        PlanConfig.class,
        ScenarioConfig.class,
        ClusterRollupSchedulerConfig.class,
        TemplatesConfig.class,
        ApiSecurityConfig.class,
        GlobalConfig.class,
        SQLDatabaseConfig.class,
        PlanProjectSchedulerConfig.class,
        PlanProjectConfig.class,
        ReservationConfig.class,
        PlanDeletionSchedulerConfig.class})
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

    @Autowired
    private PlanProjectConfig planProjectConfig;

    @Autowired
    private ReservationConfig reservationConfig;

    @Autowired
    private PlanProjectSchedulerConfig schedulerConfig;

    @Autowired
    private PlanDeletionSchedulerConfig planDeletionSchedulerConfig;

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
        planDeletionSchedulerConfig.planDeletionTask().start();
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
                .addService(planProjectConfig.planProjectService())
                .addService(reservationConfig.reservationRpcService())
                .build());
    }
}
