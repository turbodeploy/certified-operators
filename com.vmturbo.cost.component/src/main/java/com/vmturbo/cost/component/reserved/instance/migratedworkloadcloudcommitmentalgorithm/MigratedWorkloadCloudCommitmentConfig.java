package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.HistoryServiceConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.PlanOrchestratorConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.cost.component.plan.PlanService;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSenderConfig;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanActionContextRiBuyStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanActionContextRiBuyStoreImpl;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanBuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanBuyReservedInstanceStoreImpl;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanProjectedEntityToReservedInstanceMappingStoreImpl;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanReservedInstanceBoughtStoreImpl;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanReservedInstanceSpecStoreImpl;
import com.vmturbo.cost.component.rpc.MigratedWorkloadCloudCommitmentAnalysisService;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Spring configuration class for the migrated workload cloud commitment resources.
 */
@Configuration
@ComponentScan
@Import({
        HistoryServiceConfig.class,
        PlanOrchestratorConfig.class,
        DbAccessConfig.class,
        ReservedInstanceActionsSenderConfig.class,
        PricingConfig.class,
        IdentityProviderConfig.class})
public class MigratedWorkloadCloudCommitmentConfig {
    /**
     * The DBAccessConfig includes a DSLContext that will be wired into the repository classes, used for querying the
     * cost database.
     */
    @Autowired
    private DbAccessConfig dbAccessConfig;

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
     * Instantiate planBuyReservedInstanceStoreImpl bean.
     *
     * @return the planBuyReservedInstanceStoreImpl bean.
     */
    @Bean
    public PlanBuyReservedInstanceStore planBuyReservedInstanceStoreImpl() {
        try {
            return new PlanBuyReservedInstanceStoreImpl(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PlanBuyReservedInstanceStoreImpl bean", e);
        }
    }

    /**
     * Instantiate planReservedInstanceSpecStoreImpl bean.
     *
     * @return the planReservedInstanceSpecStoreImpl bean.
     */
    @Bean
    public PlanReservedInstanceSpecStore planReservedInstanceSpecStoreImpl() {
        try {
            return new PlanReservedInstanceSpecStoreImpl(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create planReservedInstanceSpecStoreImpl bean", e);
        }
    }

    /**
     * Instantiate planActionContextRiBuyStoreImpl bean.
     *
     * @return the planActionContextRiBuyStoreImpl bean.
     */
    @Bean
    public PlanActionContextRiBuyStore planActionContextRiBuyStoreImpl() {
        try {
            return new PlanActionContextRiBuyStoreImpl(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create planActionContextRiBuyStoreImpl bean", e);
        }
    }

    /**
     * Instantiate planProjectedEntityToReservedInstanceMappingStoreImpl bean.
     *
     * @return the planProjectedEntityToReservedInstanceMappingStoreImpl bean.
     */
    @Bean
    public PlanProjectedEntityToReservedInstanceMappingStoreImpl planProjectedEntityToReservedInstanceMappingStoreImpl() {
        try {
            return new PlanProjectedEntityToReservedInstanceMappingStoreImpl(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create planProjectedEntityToReservedInstanceMappingStoreImpl bean", e);
        }
    }

    /**
     * Instantiate planReservedInstanceBoughtStoreImpl bean.
     *
     * @return the planReservedInstanceBoughtStoreImpl bean.
     */
    @Bean
    public PlanReservedInstanceBoughtStoreImpl planReservedInstanceBoughtStoreImpl() {
        try {
            return new PlanReservedInstanceBoughtStoreImpl(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PlanReservedInstanceBoughtStoreImpl bean", e);
        }
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
        return new ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy(pricingConfig.priceTableStore(),
                pricingConfig.businessAccountPriceTableKeyStore(),
                planBuyReservedInstanceStoreImpl(),
                planReservedInstanceSpecStoreImpl(),
                planActionContextRiBuyStoreImpl(),
                planProjectedEntityToReservedInstanceMappingStoreImpl(),
                planReservedInstanceBoughtStoreImpl());
    }
}
