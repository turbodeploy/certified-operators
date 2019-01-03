package com.vmturbo.group;

import java.util.Optional;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.diagnostics.GroupDiagnosticsConfig;
import com.vmturbo.group.migration.MigrationConfig;
import com.vmturbo.group.service.RpcConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration("theComponent")
@Import({IdentityProviderConfig.class,
        MigrationConfig.class,
        RpcConfig.class,
        SettingConfig.class,
        SQLDatabaseConfig.class,
        GroupApiSecurityConfig.class,
        GroupDiagnosticsConfig.class})
public class GroupComponent extends BaseVmtComponent {

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private MigrationConfig migrationConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private GroupDiagnosticsConfig diagnosticsConfig;

    private static Logger logger = LoggerFactory.getLogger(GroupComponent.class);

    @Value("${arangodbHealthCheckIntervalSeconds:60}")
    private int arangoHealthCheckIntervalSeconds;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(
            new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,dbConfig.dataSource()::getConnection));
    }

    @Override
    @Nonnull
    protected SortedMap<String, Migration> getMigrations() {
        return migrationConfig.groupMigrationsLibrary().getMigrationsList();
    }

    @Nonnull
    @Override
    protected Optional<Server> buildGrpcServer(@Nonnull ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        return Optional.of(builder
            .addService(ServerInterceptors.intercept(rpcConfig.policyService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(rpcConfig.groupService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(rpcConfig.discoveredCollectionsRpcService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(rpcConfig.settingService(), monitoringInterceptor))
            .addService(ServerInterceptors.intercept(rpcConfig.settingPolicyService(), monitoringInterceptor))
            .build());
    }

    public static void main(String[] args) {
        startContext(GroupComponent.class);
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagsHandler().dump(diagnosticZip);
    }
}
