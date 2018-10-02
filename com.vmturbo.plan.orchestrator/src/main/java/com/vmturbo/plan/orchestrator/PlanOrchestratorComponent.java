package com.vmturbo.plan.orchestrator;

import java.util.Optional;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;

import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.plan.orchestrator.cpucapacity.CpuCapacityConfig;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.diagnostics.PlanOrchestratorDiagnosticsConfig;
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
        PlanOrchestratorDiagnosticsConfig.class,
        PlanDeletionSchedulerConfig.class,
        CpuCapacityConfig.class})
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

    @Autowired
    private PlanOrchestratorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private CpuCapacityConfig cpuCapacityConfig;

    @PostConstruct
    private void setup() {
        LOGGER.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        dbConfig.dataSource()::getConnection));
        getHealthMonitor().addHealthCheck(planConfig.kafkaHealthMonitor());
    }

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

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        return Optional.of(builder
            .addService(ServerInterceptors.intercept(scenarioConfig.scenarioService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(planConfig.planService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(templatesConfig.templatesService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(templatesConfig.templateSpecService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(templatesConfig.discoveredTemplateDeploymentProfileService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(deploymentProfileConfig.deploymentProfileRpcService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(planProjectConfig.planProjectService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(reservationConfig.reservationRpcService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(cpuCapacityConfig.cpuCapacityService(), monitoringInterceptor))
            .build());
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnosticsHandler().dump(diagnosticZip);
    }
}
