package com.vmturbo.plan.orchestrator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.plan.orchestrator.cpucapacity.CpuCapacityConfig;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.diagnostics.PlanOrchestratorDiagnosticsConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.plan.orchestrator.reservation.ReservationConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.scheduled.ClusterRollupSchedulerConfig;
import com.vmturbo.plan.orchestrator.scheduled.MigrationConfig;
import com.vmturbo.plan.orchestrator.scheduled.PlanDeletionSchedulerConfig;
import com.vmturbo.plan.orchestrator.scheduled.PlanProjectSchedulerConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Responsible for orchestrating plan workflow.
 */
@Configuration("theComponent")
@Import({DeploymentProfileConfig.class,
        PlanConfig.class,
        ScenarioConfig.class,
        ClusterRollupSchedulerConfig.class,
        TemplatesConfig.class,
        ApiSecurityConfig.class,
        GlobalConfig.class,
        PlanOrchestratorDBConfig.class,
        PlanProjectSchedulerConfig.class,
        PlanProjectConfig.class,
        ReservationConfig.class,
        PlanOrchestratorDiagnosticsConfig.class,
        PlanDeletionSchedulerConfig.class,
        CpuCapacityConfig.class,
        SpringSecurityConfig.class,
        MigrationConfig.class})
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

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

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

    @Autowired
    private PlanOrchestratorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private CpuCapacityConfig cpuCapacityConfig;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    /**
     * Provides the migrations library for migrating old plans.
     */
    @Autowired
    private MigrationConfig migrationConfig;

    @PostConstruct
    private void setup() {
        LOGGER.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
            dbConfig.dataSource()::getConnection));
        getHealthMonitor().addHealthCheck(planConfig.kafkaHealthMonitor());
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(PlanOrchestratorComponent.class);
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


    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
            return Arrays.asList(scenarioConfig.scenarioService(),
            planConfig.planService(),
            templatesConfig.templatesService(),
            templatesConfig.templateSpecService(),
            templatesConfig.discoveredTemplateDeploymentProfileService(),
            deploymentProfileConfig.deploymentProfileRpcService(),
            planProjectConfig.planProjectService(),
            reservationConfig.reservationRpcService(),
            cpuCapacityConfig.cpuCapacityService());
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnosticsHandler().dump(diagnosticZip);
    }

    @Nonnull
    @Override
    protected SortedMap<String, Migration> getMigrations() {
        return migrationConfig.planOrchestratorMigrationsLibrary().getMigrations();
    }
}
