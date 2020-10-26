package com.vmturbo.cost.component.topology;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory.DefaultTopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.SupplyChainServiceConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.discount.DiscountConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Setup listener for topologies from Topology Processor. Does not directly configured
 * the listener of the topology processor
 */

@Configuration
@Import({ComputeTierDemandStatsConfig.class,
        TopologyProcessorListenerConfig.class,
        PricingConfig.class,
        EntityCostConfig.class,
        DiscountConfig.class,
        ReservedInstanceConfig.class,
        CostConfig.class,
        RepositoryClientConfig.class,
        BuyRIAnalysisConfig.class,
        ReservedInstanceSpecConfig.class,
        CostDBConfig.class,
        SupplyChainServiceConfig.class,
        GroupClientConfig.class,
        CloudCommitmentAnalysisStoreConfig.class,
        CostClientConfig.class})
public class TopologyListenerConfig {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private DiscountConfig discountConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private CostConfig costConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private SupplyChainServiceConfig supplyChainServiceConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private CloudCommitmentAnalysisStoreConfig cloudCommitmentAnalysisStoreConfig;

    @Autowired
    private BuyRIAnalysisConfig buyRIAnalysisConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${maxTrackedLiveTopologies:10}")
    private int maxTrackedLiveTopologies;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        final LiveTopologyEntitiesListener entitiesListener =
                new LiveTopologyEntitiesListener(
                        computeTierDemandStatsConfig.riDemandStatsWriter(),
                        cloudTopologyFactory(), topologyCostCalculatorFactory(),
                        entityCostConfig.entityCostStore(),
                        reservedInstanceConfig.reservedInstanceCoverageUpload(),
                        costConfig.businessAccountHelper(),
                        costJournalRecorder(),
                        buyRIAnalysisConfig.reservedInstanceAnalysisInvoker(),
                        topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                        cloudCommitmentAnalysisStoreConfig.cloudCommitmentDemandWriter());

        topologyProcessorListenerConfig.topologyProcessor()
                .addLiveTopologyListener(entitiesListener);
        return entitiesListener;
    }

    @Bean
    public PlanTopologyEntitiesListener planTopologyEntitiesListener() {
        final PlanTopologyEntitiesListener entitiesListener =
                new PlanTopologyEntitiesListener(realtimeTopologyContextId,
                        computeTierDemandStatsConfig.riDemandStatsWriter(),
                        cloudTopologyFactory(), topologyCostCalculatorFactory(),
                        entityCostConfig.entityCostStore(),
                        reservedInstanceConfig.reservedInstanceCoverageUpload(),
                        costConfig.businessAccountHelper(),
                        buyRIAnalysisConfig.reservedInstanceAnalysisInvoker());

        topologyProcessorListenerConfig.topologyProcessor()
                .addPlanTopologyListener(entitiesListener);
        return entitiesListener;
    }

    @Bean
    public TopologyProcessorNotificationListener topologyProcessorNotificationListener() {
        final TopologyProcessorNotificationListener targetListener =
                new TopologyProcessorNotificationListener(
                costConfig.businessAccountHelper(),
                pricingConfig.businessAccountPriceTableKeyStore(),
                RIAndExpenseUploadServiceGrpc.newBlockingStub(costClientConfig.costChannel()));
            topologyProcessorListenerConfig.topologyProcessor()
                    .addTargetListener(targetListener);
        return targetListener;
    }

    @Bean
    public TopologyCostCalculatorFactory topologyCostCalculatorFactory() {
        return new DefaultTopologyCostCalculatorFactory(topologyEntityInfoExtractor(),
                cloudCostCalculatorFactory(), localCostDataProvider(), discountApplicatorFactory(),
                riApplicatorFactory());
    }

    /**
     * Get the price table identity key store.
     *
     * @return the price table identity key store.
     */
    @Bean
    public PriceTableKeyIdentityStore priceTableKeyIdentityStore() {
        return new PriceTableKeyIdentityStore(
                databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    /**
     * Get the business account price table key store.
     *
     * @return the business account price table key store.
     */
    @Bean
    public BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore() {
        return new BusinessAccountPriceTableKeyStore(databaseConfig.dsl(),
                priceTableKeyIdentityStore());
    }

    @Bean
    public ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory() {
        return ReservedInstanceApplicator.newFactory();
    }

    @Bean
    public DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory() {
        return DiscountApplicator.newFactory();
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                new GroupMemberRetriever(GroupServiceGrpc
                        .newBlockingStub(groupClientConfig.groupChannel())));
    }

    @Bean
    public TopologyEntityInfoExtractor topologyEntityInfoExtractor() {
        return new TopologyEntityInfoExtractor();
    }

    @Bean
    public CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory() {
        return CloudCostCalculator.newFactory();
    }

    @Bean
    public LocalCostDataProvider localCostDataProvider() {
        return new LocalCostDataProvider(pricingConfig.priceTableStore(),
                discountConfig.discountStore(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                businessAccountPriceTableKeyStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                reservedInstanceConfig.entityReservedInstanceMappingStore(),
                repositoryClientConfig.repositoryClient(),
                supplyChainServiceConfig.supplyChainRpcService(),
                realtimeTopologyContextId, identityProviderConfig.identityProvider(),
                discountApplicatorFactory(), topologyEntityInfoExtractor());
    }

    @Bean
    public CostJournalRecorder costJournalRecorder() {
        return new CostJournalRecorder();
    }

    @Bean
    public CostJournalRecorderController costJournalRecorderController() {
        return new CostJournalRecorderController(costJournalRecorder());
    }
}
