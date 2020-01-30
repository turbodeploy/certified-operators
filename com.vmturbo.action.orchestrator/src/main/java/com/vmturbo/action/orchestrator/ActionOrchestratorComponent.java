package com.vmturbo.action.orchestrator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import com.vmturbo.action.orchestrator.migration.MigrationConfig;
import com.vmturbo.components.common.migration.Migration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

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

/**
 * The component for the action orchestrator.
 */
@Configuration("theComponent")
@Import({ActionOrchestratorApiConfig.class,
        ActionOrchestratorDiagnosticsConfig.class,
        MigrationConfig.class,
        RpcConfig.class,
        NotificationsConfig.class,
        ActionExecutionConfig.class,
        MarketConfig.class,
        ActionStoreConfig.class,
        ApiSecurityConfig.class,
        ActionOrchestratorGlobalConfig.class,
        ActionOrchestratorDBConfig.class,
        SpringSecurityConfig.class,
        WorkflowConfig.class})
public class ActionOrchestratorComponent extends BaseVmtComponent {

    private Logger log = LogManager.getLogger();

    @Autowired
    private ActionOrchestratorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private ActionOrchestratorDBConfig dbConfig;

    @Autowired
    private ActionOrchestratorApiConfig actionOrchestratorApiConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private MigrationConfig migrationConfig;

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
        getHealthMonitor().addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
            dbConfig.dataSource()::getConnection));
        getHealthMonitor().addHealthCheck(actionOrchestratorApiConfig.kafkaProducerHealthMonitor());
    }

    @Override
    @Nonnull
    protected SortedMap<String, Migration> getMigrations() {
        return migrationConfig.actionsMigrationsLibrary().getMigrationsList();
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnosticsHandler().dump(diagnosticZip);
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        final List<BindableService> services = new ArrayList<>();
        services.add(rpcConfig.actionRpcService());
        services.add(rpcConfig.entitySeverityRpcService());
        services.add(rpcConfig.actionConstraintsRpcService());
        services.add(workflowConfig.discoveredWorkflowRpcService());
        services.add(workflowConfig.fetchWorkflowRpcService());
        rpcConfig.actionsDebugRpcService().ifPresent(services::add);
        return services;
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(ActionOrchestratorComponent.class);
    }
}
