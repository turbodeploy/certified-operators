package com.vmturbo.cost.component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * The main cost component.
 */
@Configuration("theComponent")
@Import({
        BuyRIAnalysisConfig.class,
        CostConfig.class,
        CostServiceConfig.class,
        PricingConfig.class,
        ReservedInstanceConfig.class,
        SQLDatabaseConfig.class,
        SpringSecurityConfig.class,
        TopologyListenerConfig.class})
public class CostComponent extends BaseVmtComponent {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger();

    @Autowired
    private SQLDatabaseConfig dbConfig;

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

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
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
    }


    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Arrays.asList(pricingConfig.pricingRpcService(),
            buyRIAnalysisConfig.buyReservedInstanceScheduleRpcService(),
            reservedInstanceConfig.reservedInstanceBoughtRpcService(),
            reservedInstanceConfig.reservedInstanceSpecRpcService(),
            costConfig.costRpcService(),
            reservedInstanceConfig.reservedInstanceUtilizationCoverageRpcService(),
            costServiceConfig.riAndExpenseUploadRpcService(),
            costServiceConfig.costDebugRpcService(),
            buyRIAnalysisConfig.buyReservedInstanceRpcService(),
            buyRIAnalysisConfig.riBuyContextFetchRpcService());
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }
}
