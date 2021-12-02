package com.vmturbo.group;

import java.sql.SQLException;
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
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.diagnostics.GroupDiagnosticsConfig;
import com.vmturbo.group.pipeline.PipelineConfig;
import com.vmturbo.group.schedule.ScheduleConfig;
import com.vmturbo.group.service.RpcConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Main component configuration for the Group Component. Manages groups and policies.
 */
@Configuration("theComponent")
@Import({IdentityProviderConfig.class,
        RpcConfig.class,
        SettingConfig.class,
        ScheduleConfig.class,
        DbAccessConfig.class,
        GroupApiSecurityConfig.class,
        GroupDiagnosticsConfig.class,
        PlanOrchestratorClientConfig.class,
        SpringSecurityConfig.class,
        PipelineConfig.class})
public class GroupComponent extends BaseVmtComponent {

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private RpcConfig rpcConfig;

    @Autowired
    private DbAccessConfig dbConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private ScheduleConfig scheduleConfig;

    @Autowired
    private GroupDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private PipelineConfig pipelineConfig;

    private static Logger logger = LoggerFactory.getLogger(GroupComponent.class);

    @Value("${arangodbHealthCheckIntervalSeconds:60}")
    private int arangoHealthCheckIntervalSeconds;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariadbHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        try {
            logger.info("Adding {} health check to the component health monitor.", dbConfig.dsl().dialect().getName());
            getHealthMonitor().addHealthCheck(
                    new SQLDBHealthMonitor(dbConfig.dsl().dialect().getName(), mariadbHealthCheckIntervalSeconds, dbConfig.dataSource()::getConnection));
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SQLDBHealthMonitor", e);
        }
        dbConfig.startDbMonitor();
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
            rpcConfig.topologyDataDefinitionRpcService(),
            rpcConfig.entityCustomTagsService());
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
        runComponent(GroupComponent.class);
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagsHandler().dump(diagnosticZip);
    }
}
