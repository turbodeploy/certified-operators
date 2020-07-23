package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.HistoryServiceConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.PlanOrchestratorConfig;
import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.cost.component.plan.PlanService;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSenderConfig;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanActionContextRiBuyStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanBuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanReservedInstanceSpecStore;
import com.vmturbo.cost.component.rpc.MigratedWorkloadCloudCommitmentAnalysisService;

/**
 * Spring configuration class for the migrated workload cloud commitment resources.
 */
@Configuration
@ComponentScan
@Import({
        HistoryServiceConfig.class,
        PlanOrchestratorConfig.class,
        CostDBConfig.class,
        ReservedInstanceActionsSenderConfig.class,
        PricingConfig.class,
        IdentityProviderConfig.class})
public class MigratedWorkloadCloudCommitmentConfig {
    /**
     * The CostDBConfig includes a DSLContext that will be wired into the repository classes, used for querying the
     * cost database.
     */
    @Autowired
    private CostDBConfig databaseConfig;

    /**
     * Provides access to the cost component's identify generator.
     */
    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    /**
     * Provides access to the MigratedWorkloadCloudCommitmentAnalysisService to publish its action plan to the
     * action orchestrator.
     */
    @Autowired
    private ReservedInstanceActionsSenderConfig reservedInstanceActionsSenderConfig;

    /**
     * Repository used to insert BuyReservedInstance objects into the buy_reserved_instance cost database table.
     */
    @Autowired
    private PlanBuyReservedInstanceStore planBuyReservedInstanceStore;

    /**
     * Repository used to query the reserved_instance_spec table to match a region, compute tier, and migration
     * profile to its reserved instance spec.
     */
    @Autowired
    private PlanReservedInstanceSpecStore planReservedInstanceSpecStore;

    /**
     * Repository used to insert records into the action_context_ri_buy table.
     */
    @Autowired
    private PlanActionContextRiBuyStore planActionContextRiBuyStore;

    /**
     * The plan service, which is needed by the MigratedWorkloadCloudCommitmentAnalysisService to retrieve PlanInstances.
     */
    @Autowired
    private PlanService planService;

    /**
     * Provides access to the business account price table key store, which is used to find the price table key for
     * a business account, and the price table store, which is used to retrieve on-demand and reserved instance prices.
     */
    @Autowired
    private PricingConfig pricingConfig;

    /**
     * gRPC service endpoint that handles migrated workload cloud commitment (Buy RI) analysis.
     *
     * @return The Spring Bean that implements this functionality.
     */
    @Bean
    public MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService() {
        return new MigratedWorkloadCloudCommitmentAnalysisService(reservedInstanceActionsSenderConfig.actionSender(), planService);
    }

    /**
     * The migrated workload cloud commitment (Buy RI) algorithm implementation. This bean will be autowired into the
     * MigratedWorkloadCloudCommitmentAnalysisService and used to analyze a plan topology for recommended cloud
     * commitment purchases.
     *
     * @param historicalStatsService The historical stats service, used to retrieve historical statistics for our migrated VMs     *
     * @return The algorithm implementation for this strategy.
     */
    @Bean
    public MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy(HistoricalStatsService historicalStatsService) {
        return new ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy(historicalStatsService,
                pricingConfig.priceTableStore(),
                pricingConfig.businessAccountPriceTableKeyStore(),
                planBuyReservedInstanceStore,
                planReservedInstanceSpecStore,
                planActionContextRiBuyStore);
    }
}
