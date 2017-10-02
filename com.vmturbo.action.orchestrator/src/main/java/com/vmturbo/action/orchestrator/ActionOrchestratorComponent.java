package com.vmturbo.action.orchestrator;

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

import com.vmturbo.action.orchestrator.api.ApiSecurityConfig;
import com.vmturbo.action.orchestrator.diagnostics.ActionOrchestratorDiagnosticsConfig;
import com.vmturbo.action.orchestrator.rpc.RpcConfig;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;

/**
 * The component for the action orchestrator.
 */
@Configuration("theComponent")
@Import({ActionOrchestratorDiagnosticsConfig.class,
        RpcConfig.class,
        ApiSecurityConfig.class,
        SQLDatabaseConfig.class})
@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan({"com.vmturbo.action.orchestrator"})
public class ActionOrchestratorComponent extends BaseVmtComponent {

    private Logger log = LogManager.getLogger();

    @Autowired
    private ActionOrchestratorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Value("${spring.application.name}")
    private String componentName;

    @Override
    public String getComponentName() {
        return componentName;
    }

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck("MariaDB",
                new SQLDBHealthMonitor(mariaHealthCheckIntervalSeconds,dbConfig.dataSource()::getConnection));
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnostics().dump(diagnosticZip);
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        builder
            .addService(rpcConfig.actionRpcService())
            .addService(rpcConfig.entitySeverityRpcService());
        rpcConfig.actionsDebugRpcService().ifPresent(builder::addService);

        return Optional.of(builder.build());
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(ActionOrchestratorComponent.class)
                .run(args);
    }
}
