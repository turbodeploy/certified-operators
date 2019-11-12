package com.vmturbo.cost.component.reserved.instance;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceBoughtServiceController;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceSpecServiceController;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceUtilizationCoverageServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalRICoverageAnalysisFactory;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

@Configuration
@Import({IdentityProviderConfig.class,
    GroupClientConfig.class,
    MarketClientConfig.class,
    MarketListenerConfig.class,
    SQLDatabaseConfig.class,
    RepositoryClientConfig.class,
    ComputeTierDemandStatsConfig.class,
    CostNotificationConfig.class,
    CostComponentGlobalConfig.class,
    TopologyProcessorListenerConfig.class})
public class ReservedInstanceConfig {

    @Value("${retention.numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds}")
    private int updateRetentionIntervalSeconds;

    @Value("${riCoverageCacheExpireMinutes:120}")
    private int riCoverageCacheExpireMinutes;

    @Value("${projectedTopologyListenerTimeOut}")
    private int projectedTopologyTimeOut;

    @Value("${persistEntityCostChunkSize}")
    private int persistEntityCostChunkSize;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${supplementalRICoverageValidation:false}")
    private boolean supplementalRICoverageValidation;

    @Value("${concurrentSupplementalRICoverageAllocation:true}")
    private boolean concurrentSupplementalRICoverageAllocation;

    /**
     * Max size of a batch to insert into 'reserved_instance_spec'.
     */
    @Value("${riSpecStoreInsertBatch:10000}")
    private int riBatchSize;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private MarketComponent marketComponent;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

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

    @Bean
    public ReservedInstanceBoughtStore reservedInstanceBoughtStore() {
        return new ReservedInstanceBoughtStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public BuyReservedInstanceStore buyReservedInstanceStore() {
        return new BuyReservedInstanceStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public ReservedInstanceSpecStore reservedInstanceSpecStore() {
        return new ReservedInstanceSpecStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider(), riBatchSize);
    }

    @Bean
    public EntityReservedInstanceMappingStore entityReservedInstanceMappingStore() {
        return new EntityReservedInstanceMappingStore(databaseConfig.dsl());
    }

    @Bean
    public ReservedInstanceUtilizationStore reservedInstanceUtilizationStore() {
        return new ReservedInstanceUtilizationStore(databaseConfig.dsl(),
                reservedInstanceBoughtStore(), reservedInstanceSpecStore(),
                entityReservedInstanceMappingStore());
    }

    @Bean
    public ReservedInstanceCoverageStore reservedInstanceCoverageStore() {
        return new ReservedInstanceCoverageStore(databaseConfig.dsl());
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    /**
     *  ReservedInstanceBoughtRpcService bean.
     * @return The {@link ReservedInstanceBoughtRpcService}
     */
    @Bean
    public ReservedInstanceBoughtRpcService reservedInstanceBoughtRpcService() {
        return new ReservedInstanceBoughtRpcService(reservedInstanceBoughtStore(),
                entityReservedInstanceMappingStore(), repositoryClientConfig.repositoryClient(),
                supplyChainRpcService(),
                realtimeTopologyContextId);
    }

    @Bean
    public ReservedInstanceSpecRpcService reservedInstanceSpecRpcService() {
        return new ReservedInstanceSpecRpcService(reservedInstanceSpecStore(), databaseConfig.dsl());
    }

    @Bean
    public ProjectedRICoverageAndUtilStore projectedEntityRICoverageAndUtilStore() {
        return new ProjectedRICoverageAndUtilStore(repositoryClientConfig.repositoryClient(),
                        supplyChainRpcService());
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
            timeFrameCalculator(), costComponentGlobalConfig.clock());
    }

    @Bean
    public ReservedInstanceBoughtServiceController reservedInstanceBoughtServiceController() {
        return new ReservedInstanceBoughtServiceController(reservedInstanceBoughtRpcService());
    }

    @Bean
    public ReservedInstanceSpecServiceController reservedInstanceSpecServiceController() {
        return new ReservedInstanceSpecServiceController(reservedInstanceSpecRpcService());
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
        return new DefaultTopologyEntityCloudTopologyFactory();
    }

    /**
     * PlanProjectedRICoverageAndUtilStore bean.
     * @return The {@link PlanProjectedRICoverageAndUtilStore}
     */
    @Bean
    public PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore() {
        PlanProjectedRICoverageAndUtilStore PlanProjectedRICoverageAndUtilStore
                = new PlanProjectedRICoverageAndUtilStore(databaseConfig.dsl(),
                                                   projectedTopologyTimeOut,
                                                   repositoryServiceClient(),
                                                   repositoryClientConfig.repositoryClient(),
                                                   reservedInstanceBoughtStore(),
                                                   reservedInstanceSpecStore(),
                                                   supplyChainRpcService(),
                                                   persistEntityCostChunkSize,
                                                   realtimeTopologyContextId);
        repositoryClientConfig.repository().addListener(PlanProjectedRICoverageAndUtilStore);
        return PlanProjectedRICoverageAndUtilStore;
    }

    @Bean
    public ActionContextRIBuyStore actionContextRIBuyStore() {
        return new ActionContextRIBuyStore(databaseConfig.dsl());
    }

    @Bean
    public ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory() {
        return new ReservedInstanceCoverageValidatorFactory(
                reservedInstanceBoughtStore(),
                reservedInstanceSpecStore());

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
    public SupplementalRICoverageAnalysisFactory supplementalRICoverageAnalysisFactory() {
        return new SupplementalRICoverageAnalysisFactory(
                coverageTopologyFactory(),
                reservedInstanceBoughtStore(),
                reservedInstanceSpecStore(),
                supplementalRICoverageValidation,
                concurrentSupplementalRICoverageAllocation);
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryServiceClient() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }
}
