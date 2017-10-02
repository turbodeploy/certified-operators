package com.vmturbo.topology.processor;

import java.util.Optional;
import java.util.zip.ZipOutputStream;

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
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;
import com.vmturbo.topology.processor.actions.ActionsConfig;
import com.vmturbo.topology.processor.analysis.AnalysisConfig;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.rpc.TopologyProcessorRpcConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * The main class of the Topology Processor.
 * <p>
 * Responsible for startup, Spring integration, configuration, and all
 * that jazz.
 */
@Configuration("theComponent")
@Import({
    TopologyProcessorDiagnosticsConfig.class,
    AnalysisConfig.class,
    ActionsConfig.class,
    SchedulerConfig.class,
    EntityConfig.class,
    TopologyConfig.class,
    IdentityProviderConfig.class,
    TopologyProcessorRpcConfig.class,
    SQLDatabaseConfig.class,
})
@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan({"com.vmturbo.topology.processor"})
public class TopologyProcessorComponent extends BaseVmtComponent {

    private Logger log = LogManager.getLogger();

    @Autowired
    private TopologyProcessorDiagnosticsConfig diagsConfig;

    @Autowired
    private AnalysisConfig analysisConfig;

    @Autowired
    private ActionsConfig actionsConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private TopologyProcessorRpcConfig topologyProcessorRpcConfig;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Value("${spring.application.name}")
    private String componentName;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck("MariaDB",
                new SQLDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        dbConfig.dataSource()::getConnection));
    }

    @Override
    public String getComponentName() {
        return componentName;
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagsConfig.diagsHandler().dumpDiags(diagnosticZip);
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        return Optional.of(builder.addService(analysisConfig.analysisService())
            .addService(actionsConfig.actionExecutionService())
            .addService(schedulerConfig.scheduleRpcService())
            .addService(entityConfig.entityInfoRpcService())
            .addService(topologyConfig.topologyRpcService())
            .addService(identityProviderConfig.identityRpcService())
            .addService(topologyProcessorRpcConfig.discoveredGroupRpcService())
            .build());
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(TopologyProcessorComponent.class)
                .run(args);
    }
}
