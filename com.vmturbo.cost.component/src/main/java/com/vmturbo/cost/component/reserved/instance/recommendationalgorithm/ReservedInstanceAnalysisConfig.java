package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.spec.catalog.ReservedInstanceCatalog.ReservedInstanceCatalogFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.cost.component.CostServiceConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSenderConfig;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyAnalysisContextProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyHistoricalDemandProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator.RIBuyDemandCalculatorFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RISpecPurchaseFilter.ReservedInstanceSpecPurchaseFilterFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCacheFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceCatalogMatcher.ReservedInstanceCatalogMatcherFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceInventoryMatcherFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RISpecMatcherFactory;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

@Import({
        ComputeTierDemandStatsConfig.class,
        CostConfig.class,
        CostServiceConfig.class,
        GroupClientConfig.class,
        PricingConfig.class,
        RepositoryClientConfig.class,
        ReservedInstanceConfig.class,
        ReservedInstanceActionsSenderConfig.class,
        ReservedInstanceSpecConfig.class})
@Configuration
public class ReservedInstanceAnalysisConfig {

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${preferredCurrentWeight:0.6}")
    private float preferredCurrentWeight;

    @Value("${riMinimumDataPoints:168}")
    private int riMinimumDataPoints;

    @Value("${allowStandaloneAccountRIBuyAnalysis:false}")
    private boolean allowStandaloneAccountRIBuyAnalysis;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private ReservedInstanceActionsSenderConfig reservedInstanceActionsSenderConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private IdentityProvider identityProvider;

    // Defined in LocalCloudCommitmentAnalysisConfig
    @Autowired
    private ReservedInstanceCatalogFactory reservedInstanceCatalogFactory;

    @Autowired
    private ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public ReservedInstanceAnalyzer reservedInstanceAnalyzer() {
        return new ReservedInstanceAnalyzer(
                settingServiceClient(),
                pricingConfig.priceTableStore(),
                pricingConfig.businessAccountPriceTableKeyStore(),
                riBuyAnalysisContextProvider(),
                riBuyDemandCalculatorFactory(),
                reservedInstanceActionsSenderConfig.actionSender(),
                reservedInstanceConfig.buyReservedInstanceStore(),
                reservedInstanceConfig.actionContextRIBuyStore(),
                identityProvider,
                realtimeTopologyContextId,
                riMinimumDataPoints,
                reservedInstanceBoughtStore());
    }

    @Bean
    public RIBuyAnalysisContextProvider riBuyAnalysisContextProvider() {
        return new RIBuyAnalysisContextProvider(
                computeTierDemandStatsConfig.riDemandStatsStore(),
                reservedInstanceCatalogMatcherFactory(),
                realtimeTopologyContextId,
                allowStandaloneAccountRIBuyAnalysis);
    }

    @Bean
    public RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory() {
        return new RegionalRIMatcherCacheFactory(
                riSpecMatcherFactory(),
                reservedInstanceInventoryMatcherFactory(),
                computeTierFamilyResolverFactory);
    }

    @Bean
    public RISpecMatcherFactory riSpecMatcherFactory() {
        return new RISpecMatcherFactory(
                reservedInstanceSpecConfig.reservedInstanceSpecStore());
    }

    @Bean
    public ReservedInstanceInventoryMatcherFactory reservedInstanceInventoryMatcherFactory() {
        return new ReservedInstanceInventoryMatcherFactory(
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                reservedInstanceConfig.planReservedInstanceStore());
    }

    @Bean
    public ReservedInstanceSpecPurchaseFilterFactory reservedInstanceSpecPurchaseFilterFactory() {
        return new ReservedInstanceSpecPurchaseFilterFactory();
    }

    @Bean
    public ReservedInstanceCatalogMatcherFactory reservedInstanceCatalogMatcherFactory() {
        return new ReservedInstanceCatalogMatcherFactory(
                reservedInstanceSpecPurchaseFilterFactory(),
                reservedInstanceCatalogFactory);
    }

    @Bean
    public RIBuyDemandCalculatorFactory riBuyDemandCalculatorFactory() {
        return new RIBuyDemandCalculatorFactory(
                regionalRIMatcherCacheFactory(),
                riBuyAnalysisDemandProvider(),
                reservedInstanceBoughtStore(),
                preferredCurrentWeight);
    }

    @Bean
    public RIBuyHistoricalDemandProvider riBuyAnalysisDemandProvider() {
        return new RIBuyHistoricalDemandProvider(computeTierDemandStatsConfig.riDemandStatsStore());
    }

    /**
     * Get the real-time reserved instance store (existing inventory).
     *
     * @return The real-time reserved instance store.
     */
    public ReservedInstanceBoughtStore reservedInstanceBoughtStore() {
        return reservedInstanceConfig.reservedInstanceBoughtStore();
    }

    /**
     * Get the plan reserved instance store.
     *
     * @return The plan reserved instance store.
     */
    public PlanReservedInstanceStore planReservedInstanceStore() {
        return reservedInstanceConfig.planReservedInstanceStore();
    }
}
