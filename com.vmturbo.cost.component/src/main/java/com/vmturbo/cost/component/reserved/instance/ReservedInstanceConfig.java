package com.vmturbo.cost.component.reserved.instance;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceBoughtServiceController;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceUtilizationCoverageServiceController;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.cost.component.SupplyChainServiceConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalRICoverageAnalysisFactory;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory.DefaultRICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

@Configuration
@Import({IdentityProviderConfig.class,
    GroupClientConfig.class,
    MarketClientConfig.class,
    MarketListenerConfig.class,
    CostDBConfig.class,
    RepositoryClientConfig.class,
    ComputeTierDemandStatsConfig.class,
    CostNotificationConfig.class,
    CostComponentGlobalConfig.class,
    TopologyProcessorListenerConfig.class,
    SupplyChainServiceConfig.class,
    CostClientConfig.class,
    EntityCostConfig.class})
public class ReservedInstanceConfig {

    @Value("${retention.numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds}")
    private int updateRetentionIntervalSeconds;

    @Value("${riCoverageCacheExpireMinutes:120}")
    private int riCoverageCacheExpireMinutes;

    @Value("${persistEntityCostChunkSize}")
    private int persistEntityCostChunkSize;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${supplementalRICoverageValidation:false}")
    private boolean supplementalRICoverageValidation;

    @Value("${concurrentSupplementalRICoverageAllocation:true}")
    private boolean concurrentSupplementalRICoverageAllocation;

    @Value("${ignoreReservedInstanceInventory:false}")
    private boolean ignoreReservedInstanceInventory;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private MarketComponent marketComponent;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private CostComponentGlobalConfig costComponentGlobalConfig;

    @Autowired
    private CostNotificationConfig costNotificationConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private SupplyChainServiceConfig supplyChainRpcServiceConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private CostConfig costConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Bean
    public ReservedInstanceBoughtStore reservedInstanceBoughtStore() {
        if (ignoreReservedInstanceInventory) {
            return new EmptyReservedInstanceBoughtStore();
        } else {
            return new SQLReservedInstanceBoughtStore(databaseConfig.dsl(),
                    identityProviderConfig.identityProvider(), repositoryInstanceCostCalculator(),
                    pricingConfig.priceTableStore(),
                    entityReservedInstanceMappingStore(),
                    accountRIMappingStore(),
                    costConfig.businessAccountHelper());
        }
    }

    /**
     * Plan reserved instance store. Used to interact with plan reserved instance table.
     *
     * @return The {@link PlanReservedInstanceStore}.
     */
    @Bean
    public PlanReservedInstanceStore planReservedInstanceStore() {
        return new PlanReservedInstanceStore(databaseConfig.dsl(), identityProviderConfig.identityProvider(),
                        repositoryInstanceCostCalculator(),
                costConfig.businessAccountHelper(), entityReservedInstanceMappingStore(), accountRIMappingStore());
    }

    @Bean
    public BuyReservedInstanceStore buyReservedInstanceStore() {
        return new BuyReservedInstanceStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public EntityReservedInstanceMappingStore entityReservedInstanceMappingStore() {
        return new EntityReservedInstanceMappingStore(databaseConfig.dsl());
    }

    @Bean
    public AccountRIMappingStore accountRIMappingStore() {
        return new AccountRIMappingStore(databaseConfig.dsl());
    }

    @Bean
    public ReservedInstanceUtilizationStore reservedInstanceUtilizationStore() {
        return new ReservedInstanceUtilizationStore(databaseConfig.dsl(),
                reservedInstanceBoughtStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                entityReservedInstanceMappingStore());
    }

    @Bean
    public ReservedInstanceCoverageStore reservedInstanceCoverageStore() {
        return new ReservedInstanceCoverageStore(databaseConfig.dsl());
    }

    /**
     *  ReservedInstanceBoughtRpcService bean.
     * @return The {@link ReservedInstanceBoughtRpcService}
     */
    @Bean
    public ReservedInstanceBoughtRpcService reservedInstanceBoughtRpcService() {
        return new ReservedInstanceBoughtRpcService(reservedInstanceBoughtStore(),
                entityReservedInstanceMappingStore(), repositoryClientConfig.repositoryClient(),
                supplyChainRpcServiceConfig.supplyChainRpcService(),
                PlanReservedInstanceServiceGrpc.newBlockingStub(costClientConfig.costChannel()),
                realtimeTopologyContextId, pricingConfig.priceTableStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                BuyReservedInstanceServiceGrpc.newBlockingStub(costClientConfig.costChannel()),
                accountRIMappingStore(), planReservedInstanceStore(), costConfig.businessAccountHelper());
    }

    /**
     * Plan reserved instance Rpc service bean.
     *
     * @return The {@link PlanReservedInstanceRpcService}
     */
    @Bean
    public PlanReservedInstanceRpcService planReservedInstanceRpcService() {
        return new PlanReservedInstanceRpcService(planReservedInstanceStore(),
                buyReservedInstanceStore(), reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                entityCostConfig.planProjectedEntityCostStore(), planProjectedRICoverageAndUtilStore());
    }

    @Bean
    public ProjectedRICoverageAndUtilStore projectedEntityRICoverageAndUtilStore() {
        return new ProjectedRICoverageAndUtilStore(
                repositoryClientConfig.repositoryClient(),
                supplyChainRpcServiceConfig.supplyChainRpcService(),
                reservedInstanceBoughtStore(),
                buyReservedInstanceStore(),
                costComponentGlobalConfig.clock());
    }

    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(costComponentGlobalConfig.clock(), updateRetentionIntervalSeconds,
            TimeUnit.SECONDS, numRetainedMinutes,
            SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(costComponentGlobalConfig.clock(), retentionPeriodFetcher());
    }

    @Bean
    public ReservedInstanceUtilizationCoverageRpcService reservedInstanceUtilizationCoverageRpcService() {
        return new ReservedInstanceUtilizationCoverageRpcService(reservedInstanceUtilizationStore(),
                reservedInstanceCoverageStore(), projectedEntityRICoverageAndUtilStore(),
                entityReservedInstanceMappingStore(), accountRIMappingStore(),
                planProjectedRICoverageAndUtilStore(), timeFrameCalculator(),
                realtimeTopologyContextId);
    }

    @Bean
    public ReservedInstanceBoughtServiceController reservedInstanceBoughtServiceController() {
        return new ReservedInstanceBoughtServiceController(reservedInstanceBoughtRpcService());
    }

    @Bean
    public ReservedInstanceUtilizationCoverageServiceController reservedInstanceUtilizationCoverageServiceController() {
        return new ReservedInstanceUtilizationCoverageServiceController(
                reservedInstanceUtilizationCoverageRpcService());
    }

    @Bean
    public ReservedInstanceCoverageUpdate reservedInstanceCoverageUpload() {
        return new ReservedInstanceCoverageUpdate(databaseConfig.dsl(), entityReservedInstanceMappingStore(),
                reservedInstanceUtilizationStore(), reservedInstanceCoverageStore(),
                reservedInstanceCoverageValidatorFactory(),
                supplementalRICoverageAnalysisFactory(),
                costNotificationConfig.costNotificationSender(),
                riCoverageCacheExpireMinutes);
    }

    /**
     * Returns the projected RI coverage listener.
     *
     * @return The projected RI coverage listener.
     */
    @Bean
    public ProjectedRICoverageListener projectedRICoverageListener() {
        final ProjectedRICoverageListener projectedRICoverageListener =
                new ProjectedRICoverageListener(projectedEntityRICoverageAndUtilStore(),
                        planProjectedRICoverageAndUtilStore(),
                        costNotificationConfig.costNotificationSender());
        marketComponent.addProjectedEntityRiCoverageListener(projectedRICoverageListener);
        return projectedRICoverageListener;
    }

    /**
     * Setup subscription for projected topology from market.
     *
     * @return The listener in the cost component that handles projected topology changes.
     */
    @Bean
    public CostComponentProjectedEntityTopologyListener projectedEntityTopologyListener() {
        final CostComponentProjectedEntityTopologyListener projectedEntityTopologyListener =
                new CostComponentProjectedEntityTopologyListener(realtimeTopologyContextId,
                        computeTierDemandStatsConfig.riDemandStatsWriter(),
                        cloudTopologyFactory());
        marketComponent.addProjectedTopologyListener(projectedEntityTopologyListener);
        return projectedEntityTopologyListener;
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                new GroupMemberRetriever(GroupServiceGrpc
                        .newBlockingStub(groupClientConfig.groupChannel())));
    }

    /**
     * PlanProjectedRICoverageAndUtilStore bean.
     * @return The {@link PlanProjectedRICoverageAndUtilStore}
     */
    @Bean
    public PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore() {
        final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore
                = new PlanProjectedRICoverageAndUtilStore(databaseConfig.dsl(),
                                                   repositoryServiceClient(),
                                                   planReservedInstanceService(),
                                                   reservedInstanceSpecConfig
                                                           .reservedInstanceSpecStore(),
                                                   persistEntityCostChunkSize);
        repositoryClientConfig.repository().addListener(planProjectedRICoverageAndUtilStore);
        return planProjectedRICoverageAndUtilStore;
    }

    @Bean
    public ActionContextRIBuyStore actionContextRIBuyStore() {
        return new ActionContextRIBuyStore(databaseConfig.dsl());
    }

    @Bean
    public ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory() {
        return new ReservedInstanceCoverageValidatorFactory(
                reservedInstanceBoughtStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore());
    }

    @Bean
    public ThinTargetCache thinTargetCache() {
        return new ThinTargetCache(topologyProcessorListenerConfig.topologyProcessor());
    }

    @Bean
    public CoverageTopologyFactory coverageTopologyFactory() {
        return new CoverageTopologyFactory(thinTargetCache());
    }

    @Bean
    public RICoverageAllocatorFactory riCoverageAllocatorFactory() {
        return new DefaultRICoverageAllocatorFactory();
    }

    @Bean
    public SupplementalRICoverageAnalysisFactory supplementalRICoverageAnalysisFactory() {
        return new SupplementalRICoverageAnalysisFactory(
                riCoverageAllocatorFactory(),
                coverageTopologyFactory(),
                reservedInstanceBoughtStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                supplementalRICoverageValidation,
                concurrentSupplementalRICoverageAllocation,
                accountRIMappingStore());
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryServiceClient() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    /**
     * Gets the plan service handle.
     *
     * @return plan service handle.
     */
    @Bean
    public PlanReservedInstanceServiceBlockingStub planReservedInstanceService() {
        return PlanReservedInstanceServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Instantiate bean for ReservedInstanceCostCalculator class.
     *
     * @return object of type ReservedInstanceCostCalculator.
     */
    @Bean
    public ReservedInstanceCostCalculator repositoryInstanceCostCalculator() {
        return new ReservedInstanceCostCalculator(reservedInstanceSpecConfig
                .reservedInstanceSpecStore());
    }
}
