package com.vmturbo.topology.processor;

import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.servlet.ServletException;

import io.grpc.BindableService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.ConfigurableWebApplicationContext;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.topology.processor.actions.ActionsConfig;
import com.vmturbo.topology.processor.analysis.AnalysisConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiSecurityConfig;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.cpucapacity.CpuCapacityConfig;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.migration.MigrationsConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.planexport.PlanExportConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.rest.RESTConfig;
import com.vmturbo.topology.processor.rpc.TopologyProcessorRpcConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidationConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineBlockingConfig;

/**
 * The main class of the Topology Processor.
 * <p>
 * Responsible for startup, Spring integration, configuration, and all
 * that jazz.
 */
@Configuration("theComponent")
@Import({
    ActionsConfig.class,
    AnalysisConfig.class,
    ClockConfig.class,
    CpuCapacityConfig.class,
    EntityConfig.class,
    GlobalConfig.class,
    GroupConfig.class,
    IdentityProviderConfig.class,
    KVConfig.class,
    MigrationsConfig.class,
    OperationConfig.class,
    PlanExportConfig.class,
    ProbeConfig.class,
    RepositoryConfig.class,
    RESTConfig.class,
    SdkServerConfig.class,
    SupplyChainValidationConfig.class,
    SchedulerConfig.class,
    StitchingConfig.class,
    TargetConfig.class,
    TemplateConfig.class,
    TopologyConfig.class,
    TopologyProcessorApiConfig.class,
    TopologyProcessorApiSecurityConfig.class,
    TopologyProcessorDBConfig.class,
    TopologyProcessorDiagnosticsConfig.class,
    TopologyProcessorRpcConfig.class,
    PipelineBlockingConfig.class
})
public class TopologyProcessorComponent extends BaseVmtComponent {

    private Logger log = LogManager.getLogger();

    @Autowired
    private TopologyProcessorDiagnosticsConfig diagsConfig;

    @Autowired
    private AnalysisConfig analysisConfig;

    @Autowired
    private ActionsConfig actionsConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private MigrationsConfig migrationsConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private SdkServerConfig sdkServerConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private TopologyProcessorApiConfig topologyProcessorApiConfig;

    @Autowired
    private TopologyProcessorDBConfig topologyProcessorDBConfig;

    @Autowired
    private TopologyProcessorRpcConfig topologyProcessorRpcConfig;

    @Autowired
    private PlanExportConfig planExportConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
            topologyProcessorDBConfig.dataSource()::getConnection));
        getHealthMonitor().addHealthCheck(topologyProcessorApiConfig.messageProducerHealthMonitor());

        if (topologyProcessorDBConfig.isDbMonitorEnabled()) {
            topologyProcessorDBConfig.startDbMonitor();
        }
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagsConfig.diagsHandler().dump(diagnosticZip);
    }

    @Override
    @Nonnull
    public SortedMap<String, Migration> getMigrations() {
            return migrationsConfig.migrationsList().getMigrationsList();
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Arrays.asList(analysisConfig.analysisService(),
            actionsConfig.actionExecutionService(),
            schedulerConfig.scheduleRpcService(),
            entityConfig.entityInfoRpcService(),
            topologyProcessorRpcConfig.topologyRpcService(),
            topologyProcessorRpcConfig.stitchingJournalRpcService(),
            identityProviderConfig.identityRpcService(),
            topologyProcessorRpcConfig.discoveredGroupRpcService(),
            probeConfig.probeActionPoliciesService(),
            topologyProcessorRpcConfig.probeService(),
            topologyProcessorRpcConfig.targetRpcService(),
            planExportConfig.planExportToTargetService());
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        runComponent((contextServer) -> {
            try {
                final ConfigurableWebApplicationContext context =
                        attachSpringContext(contextServer, TopologyProcessorComponent.class);
                WebSocketServerContainerInitializer.configureContext(contextServer);
                return context;
            } catch (ServletException e) {
                throw new ContextConfigurationException("Could not configure websockets", e);
            }
        });
    }
}
