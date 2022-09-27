package com.vmturbo.voltron.extensions.tp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.DbAccessConfig;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
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
import com.vmturbo.topology.processor.planexport.PlanExportConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.rest.RESTConfig;
import com.vmturbo.topology.processor.rpc.TopologyProcessorRpcConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidationConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * The main configuration class of the Topology Processor with cache only mode enabled. The list of
 * imported configs below determines what @Configuration components are created by Spring.  We cannot
 * subclass TopologyProcessorComponent because we need define a different version of this list that
 * replaces some of the standard configs.
 */
@Configuration("Topology Processor (cache only mode)")
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
        PlanExportConfig.class,
        ProbeConfig.class,
        RepositoryConfig.class,
        RESTConfig.class,
        SdkServerConfig.class,
        SpringSecurityConfig.class,
        SupplyChainValidationConfig.class,
        StitchingConfig.class,
        TargetConfig.class,
        TemplateConfig.class,
        TopologyConfig.class,
        TopologyProcessorApiConfig.class,
        TopologyProcessorApiSecurityConfig.class,
        TopologyProcessorDBConfig.class,
        TopologyProcessorDiagnosticsConfig.class,
        TopologyProcessorRpcConfig.class,
        // Add the cache only configs
        CacheOnlyDiscoveryDumperConfig.class,
        CacheOnlyOperationConfig.class, // replaces OperationConfig.class
        CacheOnlySchedulerConfig.class, // replaces SchedulerConfig.class
        CacheOnlyPipelineBlockingConfig.class, // replaces PipelineBlockingConfig.class
})
public class CacheOnlyTopologyProcessorComponent extends BaseVmtComponent {

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
    private CacheOnlySchedulerConfig schedulerConfig;

    @Autowired
    private TopologyProcessorApiConfig topologyProcessorApiConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private TopologyProcessorRpcConfig topologyProcessorRpcConfig;

    @Autowired
    private PlanExportConfig planExportConfig;

    @Autowired
    private ApplicationContext appContext;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Value("${printBeanNames:false}")
    private boolean printBeanNames;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        try {
            getHealthMonitor().addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                    dbAccessConfig.dataSource()::getConnection));
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create MariaDBHealthMonitor", e);
        }
        getHealthMonitor().addHealthCheck(topologyProcessorApiConfig.messageProducerHealthMonitor());

        dbAccessConfig.startDbMonitor();
        if (printBeanNames) {
            printBeans();
        }
    }

    private void printBeans() {
        StringBuilder sb = new StringBuilder();
        sb.append("Spring bean names, application context ("
                + appContext.getDisplayName() + ")");
        int i = 0;
        for (String beanName : appContext.getBeanDefinitionNames()) {
            i++;
            sb.append("\n " + i + ". " + beanName);
        }
        log.info(sb.toString());
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

    @Override
    public void logInitialAuditMessage() {
        if (FeatureFlags.ENABLE_PROBE_AUTH.isEnabled()) {
            AuditLogUtils.logSecurityAudit(AuditAction.ENABLE_PROBE_SECURITY,
                getComponentName() + ": " + FeatureFlags.ENABLE_PROBE_AUTH.getName(), true);
        }
        if (FeatureFlags.ENABLE_MANDATORY_PROBE_AUTH.isEnabled()) {
            AuditLogUtils.logSecurityAudit(AuditAction.ENABLE_PROBE_SECURITY,
                getComponentName() + ": " + FeatureFlags.ENABLE_MANDATORY_PROBE_AUTH.getName(), true);
        }
    }
}
