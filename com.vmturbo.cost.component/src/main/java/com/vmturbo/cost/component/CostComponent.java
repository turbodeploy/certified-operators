package com.vmturbo.cost.component;

import java.sql.SQLException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.cost.component.billed.cost.BilledCostRpcService;
import com.vmturbo.cost.component.billed.cost.CloudCostConfig;
import com.vmturbo.cost.component.billed.cost.CloudCostDiagsRpcService;
import com.vmturbo.cost.component.cleanup.CostCleanupConfig;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentConfig;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentRpcService;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentStatsConfig;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentStatsRpcService;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentUploadRpcService;
import com.vmturbo.cost.component.db.CostPartitioningConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.db.StoredProcedureConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.flyway.CostFlywayCallback;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.MigratedWorkloadCloudCommitmentConfig;
import com.vmturbo.cost.component.rpc.CostDebugConfig;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.flyway.ForgetMigrationCallback;
import com.vmturbo.topology.event.library.uptime.EntityUptimeSpringConfig;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxThrottlingLimit;

/**
 * The main cost component.
 */
@Configuration("theComponent")
@Import({
    CostComponentGlobalConfig.class,
    CommunicationConfig.class,
    BuyRIAnalysisConfig.class,
    CostConfig.class,
    CostServiceConfig.class,
    PricingConfig.class,
    ReservedInstanceConfig.class,
    DbAccessConfig.class,
    SpringSecurityConfig.class,
    TopologyListenerConfig.class,
    CostDebugConfig.class,
    CostCleanupConfig.class,
    CostPlanListenerConfig.class,
    ReservedInstanceSpecConfig.class,
    CostDiagnosticsConfig.class,
    MigratedWorkloadCloudCommitmentConfig.class,
    EntityUptimeSpringConfig.class,
    CloudCommitmentStatsConfig.class,
    CloudCommitmentConfig.class,
    BilledCostConfig.class,
    StoredProcedureConfig.class,
    CostPartitioningConfig.class,
    CloudCostConfig.class,
    ScopeIdReplacementLogConfig.class
})
public class CostComponent extends BaseVmtComponent {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger();

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private BuyRIAnalysisConfig buyRIAnalysisConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private CostConfig costConfig;

    @Autowired
    private CostServiceConfig costServiceConfig;

    @Autowired
    private CostDiagnosticsConfig diagnosticsConfig;

    //Defined in CloudCommitmentStatsConfig
    @Autowired
    private CloudCommitmentUploadRpcService cloudCommitmentUploadRpcService;

    //Defined in CloudCommitmentStatsConfig
    @Autowired
    private CloudCommitmentStatsRpcService cloudCommitmentStatsRpcService;

    //Defined in CloudCommitmentConfig
    @Autowired
    private CloudCommitmentRpcService cloudCommitmentRpcService;

    // Partitioned cloud cost data
    @Autowired
    private BilledCostRpcService billedCostRpcService;

    // Export cloud cost diags
    @Autowired
    private CloudCostDiagsRpcService cloudCostDiagsRpcService;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Value("${defaultTraxCalculationsTrackedPerDay:360}")
    private int defaultTraxCalculationsTrackedPerDay;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private CostDebugConfig costDebugConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private MigratedWorkloadCloudCommitmentConfig migratedWorkloadCloudCommitmentConfig;

    @Autowired
    private EntityUptimeSpringConfig entityUptimeSpringConfig;

    @Autowired
    private BilledCostConfig billedCostConfig;

    @Autowired
    private ScopeIdReplacementLogConfig scopeIdReplacementLogConfig;

    @Autowired
    private CloudCostConfig cloudCostConfig;

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     * @throws ContextConfigurationException if there is an error reading the external
     * properties.yaml file
     */
    public static void main(String[] args) {
        runComponent(CostComponent.class);
    }

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        try {
            getHealthMonitor()
                    .addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                            dbAccessConfig.dataSource()::getConnection));
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create MariaDBHealthMonitor", e);
        }

        // Configure Trax to randomly sample a certain number of calculations per day for
        // tracking and debugging purposes.
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .build(),
            new TraxThrottlingLimit(defaultTraxCalculationsTrackedPerDay, Clock.systemUTC(), new Random()),
            Collections.singletonList(TraxConfiguration.DEFAULT_TOPIC_NAME)));

        dbAccessConfig.startDbMonitor();
    }

    /**
     * Define flyway callbacks to be active during migrations for cost component.
     *
     * @return array of callback objects
     */
    @Bean
    @Primary
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[] {
                new CostFlywayCallback(),
                new ForgetMigrationCallback("1.88")
        };
    }


    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Arrays.asList(pricingConfig.pricingRpcService(),
                buyRIAnalysisConfig.buyReservedInstanceScheduleRpcService(),
                reservedInstanceConfig.reservedInstanceBoughtRpcService(),
                reservedInstanceConfig.planReservedInstanceRpcService(),
                reservedInstanceSpecConfig.reservedInstanceSpecRpcService(),
                costConfig.costRpcService(),
                costConfig.reservedInstanceCostRpcService(),
                reservedInstanceConfig.reservedInstanceUtilizationCoverageRpcService(),
                costServiceConfig.riAndExpenseUploadRpcService(),
                costDebugConfig.costDebugRpcService(),
                buyRIAnalysisConfig.buyReservedInstanceRpcService(),
                buyRIAnalysisConfig.riBuyContextFetchRpcService(),
                costDebugConfig.traxConfigurationRpcService(),
                migratedWorkloadCloudCommitmentConfig.migratedWorkloadCloudCommitmentAnalysisService(),
                entityUptimeSpringConfig.entityUptimeRpcService(),
                cloudCommitmentUploadRpcService,
                cloudCommitmentStatsRpcService,
                cloudCommitmentRpcService,
                billedCostConfig.billedCostUploadRpcService(),
                billedCostRpcService,
                scopeIdReplacementLogConfig.uploadAliasedOidsRpcService(),
                cloudCostDiagsRpcService);
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagsHandler().dump(diagnosticZip);
    }
}