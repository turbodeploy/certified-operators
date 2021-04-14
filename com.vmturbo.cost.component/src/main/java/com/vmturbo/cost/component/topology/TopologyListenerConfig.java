package com.vmturbo.cost.component.topology;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

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
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.topology.cloud.listener.CCADemandCollector;
import com.vmturbo.cost.component.topology.cloud.listener.EntityCostWriter;
import com.vmturbo.cost.component.topology.cloud.listener.RIBuyRunner;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.event.library.uptime.EntityUptimeStore;

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
    private CostConfig costConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private CloudCommitmentAnalysisStoreConfig cloudCommitmentAnalysisStoreConfig;

    @Autowired
    private BuyRIAnalysisConfig buyRIAnalysisConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private CostNotificationConfig costNotificationConfig;

    @Autowired
    private EntityUptimeStore entityUptimeStore;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${maxTrackedLiveTopologies:10}")
    private int maxTrackedLiveTopologies;

    @Value("${liveTopology.cleanupInterval:PT1H}")
    private String liveTopologyCleanupInterval;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        final LiveTopologyEntitiesListener entitiesListener =
                new LiveTopologyEntitiesListener(
                        cloudTopologyFactory(),
                        reservedInstanceConfig.reservedInstanceCoverageUpload(),
                        costConfig.businessAccountHelper(),
                        topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                        ingestedTopologyStore(),
                        Arrays.asList(entityCostWriter(), riBuyRunner(), ccaDemandCollector()));

        topologyProcessorListenerConfig.topologyProcessor()
                .addLiveTopologyListener(entitiesListener);
        return entitiesListener;
    }

    @Bean(destroyMethod = "shutdown")
    protected ThreadPoolTaskScheduler liveTopologyCleanupScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        scheduler.initialize();
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("LiveTopology-cleanup-%d").build();
    }

    @Bean
    public IngestedTopologyStore ingestedTopologyStore() {
        return new IngestedTopologyStore(
                liveTopologyCleanupScheduler(),
                Duration.parse(liveTopologyCleanupInterval),
                databaseConfig.dsl());
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
                        buyRIAnalysisConfig.reservedInstanceAnalysisInvoker(),
                        costNotificationConfig.costNotificationSender());

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
                groupClientConfig.groupMemberRetriever());
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
                identityProviderConfig.identityProvider(),
                discountApplicatorFactory(), topologyEntityInfoExtractor(),
                entityUptimeStore);
    }

    @Bean
    public CostJournalRecorder costJournalRecorder() {
        return new CostJournalRecorder();
    }

    @Bean
    public CostJournalRecorderController costJournalRecorderController() {
        return new CostJournalRecorderController(costJournalRecorder());
    }

    /**
     * Bean for the entity cost writer.
     *
     * @return An instance of the entity cost writer.
     */
    @Bean
    public EntityCostWriter entityCostWriter() {
        return new EntityCostWriter(reservedInstanceConfig.reservedInstanceCoverageUpload(), topologyCostCalculatorFactory(),
                costJournalRecorder(), entityCostConfig.entityCostStore());
    }

    /**
     * Bean for the RI Buy runner.
     *
     * @return An instance of the RI Buy runner.
     */
    @Bean
    public RIBuyRunner riBuyRunner() {
        return new RIBuyRunner(buyRIAnalysisConfig.reservedInstanceAnalysisInvoker(), costConfig.businessAccountHelper(),
                computeTierDemandStatsConfig.riDemandStatsWriter());
    }

    /**
     * Bean for the CCA demand collector.
     *
     * @return An instance of the CCA demand collector.
     */
    @Bean
    public CCADemandCollector ccaDemandCollector() {
        return new CCADemandCollector(cloudCommitmentAnalysisStoreConfig.cloudCommitmentDemandWriter());
    }
}
