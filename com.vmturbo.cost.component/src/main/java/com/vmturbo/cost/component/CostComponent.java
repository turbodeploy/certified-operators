package com.vmturbo.cost.component;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.MigratedWorkloadCloudCommitmentAlgorithmStrategy;
import com.vmturbo.cost.component.rpc.MigratedWorkloadCloudCommitmentAnalysisService;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.FlywayCallback;
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
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.flyway.CostFlywayCallback;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.rpc.CostDebugConfig;
import com.vmturbo.cost.component.stats.CostStatsConfig;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxThrottlingLimit;

/**
 * The main cost component.
 */
@Configuration("theComponent")
@Import({
    CostComponentGlobalConfig.class,
    BuyRIAnalysisConfig.class,
    CostConfig.class,
    CostServiceConfig.class,
    PricingConfig.class,
    ReservedInstanceConfig.class,
    CostDBConfig.class,
    SpringSecurityConfig.class,
    TopologyListenerConfig.class,
    CostDebugConfig.class,
    CostStatsConfig.class,
    CostPlanListenerConfig.class,
    ReservedInstanceSpecConfig.class,
    CostDiagnonsticsConfig.class,
    HistoryServiceConfig.class})
public class CostComponent extends BaseVmtComponent {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger();

    @Autowired
    private CostDBConfig dbConfig;

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
    private CostDiagnonsticsConfig diagnosticsConfig;

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
    private CostStatsConfig costStatsConfig;

    @Autowired
    private CostDebugConfig costDebugConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    /**
     * gRPC service endpoint that handles migrated workload cloud commitment (Buy RI) analysis.
     * @return  The Spring Bean that implements this functionality.
     */
    @Bean
    public MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService() {
        return new MigratedWorkloadCloudCommitmentAnalysisService();
    }

    /**
     * The migrated workload cloud commitment (Buy RI) algorithm implementation. This bean will be autowired into the
     * MigratedWorkloadCloudCommitmentAnalysisService and used to analyze a plan topology for recommended cloud
     * commitment purchases.
     * @return      The algorithm implementation for this strategy.
     */
    @Bean
    public MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy(HistoricalStatsService historicalStatsService) {
        return new ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy(historicalStatsService);
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     * @throws ContextConfigurationException if there is an error reading the external
     * properties.yaml file
     */
    public static void main(String[] args) {
        startContext(CostComponent.class);
    }

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor()
                .addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                    dbConfig.dataSource()::getConnection));

        // Configure Trax to randomly sample a certain number of calculations per day for
        // tracking and debugging purposes.
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .build(),
            new TraxThrottlingLimit(defaultTraxCalculationsTrackedPerDay, Clock.systemUTC(), new Random()),
            Collections.singletonList(TraxConfiguration.DEFAULT_TOPIC_NAME)));
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
            new CostFlywayCallback()
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
            migratedWorkloadCloudCommitmentAnalysisService());
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
