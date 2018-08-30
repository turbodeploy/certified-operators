package com.vmturbo.action.orchestrator;

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

import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.api.ApiSecurityConfig;
import com.vmturbo.action.orchestrator.diagnostics.ActionOrchestratorDiagnosticsConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.notifications.NotificationsConfig;
import com.vmturbo.action.orchestrator.market.MarketConfig;
import com.vmturbo.action.orchestrator.rpc.RpcConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * The component for the action orchestrator.
 */
@Configuration("theComponent")
@Import({ActionOrchestratorApiConfig.class,
        ActionOrchestratorDiagnosticsConfig.class,
        RpcConfig.class,
        NotificationsConfig.class,
        ActionExecutionConfig.class,
        MarketConfig.class,
        ActionStoreConfig.class,
        ApiSecurityConfig.class,
        ActionOrchestratorGlobalConfig.class,
        SQLDatabaseConfig.class,
        SpringSecurityConfig.class,
        WorkflowConfig.class})
public class ActionOrchestratorComponent extends BaseVmtComponent {

    private Logger log = LogManager.getLogger();

    @Autowired
    private ActionOrchestratorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Autowired
    private ActionOrchestratorApiConfig actionOrchestratorApiConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,dbConfig.dataSource()::getConnection));
        getHealthMonitor().addHealthCheck(actionOrchestratorApiConfig.kafkaProducerHealthMonitor());
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnostics().dump(diagnosticZip);
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        // gRPC JWT token interceptor
        final JwtServerInterceptor jwtInterceptor = new JwtServerInterceptor(securityConfig.apiAuthKVStore());
        builder
            .addService(ServerInterceptors.intercept(rpcConfig.actionRpcService(),
                jwtInterceptor,
                monitoringInterceptor))
            .addService(ServerInterceptors.intercept(rpcConfig.entitySeverityRpcService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(workflowConfig.discoveredWorkflowRpcService(), monitoringInterceptor));
        rpcConfig.actionsDebugRpcService().ifPresent(actionsDebugRpcService ->
            builder.addService(ServerInterceptors.intercept(actionsDebugRpcService, monitoringInterceptor)));

        return Optional.of(builder.build());
    }

    public static void main(String[] args) {
        startContext(ActionOrchestratorComponent.class);
    }
}
