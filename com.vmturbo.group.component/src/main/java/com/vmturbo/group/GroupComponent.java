package com.vmturbo.group;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.diagnostics.GroupDiagnosticsConfig;
import com.vmturbo.group.schedule.ScheduleConfig;
import com.vmturbo.group.service.RpcConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;

/**
 * Main component configuration for the Group Component. Manages groups and policies.
 */
@Configuration("theComponent")
@Import({IdentityProviderConfig.class,
        RpcConfig.class,
        SettingConfig.class,
        ScheduleConfig.class,
        GroupComponentDBConfig.class,
        GroupApiSecurityConfig.class,
        GroupDiagnosticsConfig.class,
        PlanOrchestratorClientConfig.class,
        SpringSecurityConfig.class})
public class GroupComponent extends BaseVmtComponent {

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private GroupComponentDBConfig dbConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private ScheduleConfig scheduleConfig;

    @Autowired
    private GroupDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private SpringSecurityConfig securityConfig;

    private static Logger logger = LoggerFactory.getLogger(GroupComponent.class);

    @Value("${arangodbHealthCheckIntervalSeconds:60}")
    private int arangoHealthCheckIntervalSeconds;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
            dbConfig.dataSource()::getConnection));
    }

    @Override
    @Nonnull
    protected SortedMap<String, Migration> getMigrations() {
        /*
         There should never by any MigrationFramework-related migrations in group.
         The ones that existed previously historically (up to V_01_00_05 have been removed).
         */
        return Collections.emptySortedMap();
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Arrays.asList(rpcConfig.policyService(),
            rpcConfig.groupService(),
            rpcConfig.settingService(),
            rpcConfig.scheduleService(),
            rpcConfig.settingPolicyService(),
            rpcConfig.topologyDataDefinitionRpcService());
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }

    /**
     * Main entry point.
     *
     * @param args command-line arguments - ignored
     */
    public static void main(String[] args) {
        startContext(GroupComponent.class);
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagsHandler().dump(diagnosticZip);
    }
}
