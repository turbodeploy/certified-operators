package com.vmturbo.cost.component.reserved.instance;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceBoughtServiceController;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceUtilizationCoverageServiceController;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.cost.component.SupplyChainServiceConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentStatsConfig;
import com.vmturbo.cost.component.cloud.commitment.ProjectedCommitmentMappingProcessor;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalCoverageAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalCoverageAnalysisFactory;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({IdentityProviderConfig.class,
    GroupClientConfig.class,
    MarketClientConfig.class,
    MarketListenerConfig.class,
    DbAccessConfig.class,
    RepositoryClientConfig.class,
    ComputeTierDemandStatsConfig.class,
    CostNotificationConfig.class,
    CostComponentGlobalConfig.class,
    TopologyProcessorListenerConfig.class,
    SupplyChainServiceConfig.class,
    CostClientConfig.class,
    EntityCostConfig.class,
    SupplementalCoverageAnalysisConfig.class,
    CloudCommitmentStatsConfig.class})
public class ReservedInstanceConfig {

    @Value("${retention.numRetainedMinutes:130}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds:10}")
    private int updateRetentionIntervalSeconds;

    @Value("${riCoverageCacheExpireMinutes:120}")
    private int riCoverageCacheExpireMinutes;

    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${ignoreReservedInstanceInventory:false}")
    private boolean ignoreReservedInstanceInventory;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private MarketComponent marketComponent;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    @Autowired
    private SupplementalCoverageAnalysisFactory supplementalCoverageAnalysisFactory;

    @Autowired
    private ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor;

    @Autowired
    private SettingServiceBlockingStub settingServiceBlockingStub;

    // OM-66854 - normalization with consider the sample count of each data point within the
    // down-sampled RI coverage/utilization tables (rollup tables). If this is disabled, the behavior
    // will revert to considering each data point within a time bucket as having equal weight for
    // averaging purposes, skewing the average towards short-lived entities.
    @Value("${normalizeCouponSamples:true}")
    private boolean normalizeCouponSamples;

    // OM-66854 - normalizes the total coupon capacity and used value for down-sampled data to
    // coupon hours. Without this normalization, the total statistic will represent a total at whatever
    // the sample rate is (typically 10 minutes), which may not be useful. This is somewhat of a hack
    // and the default assumes a 10 minute broadcast interval. It is not guaranteed to always be accurate
    // given there may be manual topology broadcasts and/or broadcasts may not occur at a precise
    // 10 minute interval.
    @Value("${couponNormalizationFactor:6}")
    private float couponNormalizationFactor;

    @Bean
    public ReservedInstanceBoughtStore reservedInstanceBoughtStore() {
        if (ignoreReservedInstanceInventory) {
            return new EmptyReservedInstanceBoughtStore();
        } else {
            try {
                return new SQLReservedInstanceBoughtStore(dbAccessConfig.dsl(),
                        identityProviderConfig.identityProvider(), repositoryInstanceCostCalculator(),
                        pricingConfig.priceTableStore(), entityReservedInstanceMappingStore(),
                        accountRIMappingStore(), costConfig.businessAccountHelper());
            } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new BeanCreationException("Failed to create ReservedInstanceBoughtStore bean", e);
            }
        }
    }

    /**
     * Plan reserved instance store. Used to interact with plan reserved instance table.
     *
     * @return The {@link PlanReservedInstanceStore}.
     */
    @Bean
    public PlanReservedInstanceStore planReservedInstanceStore() {
        try {
            return new PlanReservedInstanceStore(dbAccessConfig.dsl(), identityProviderConfig.identityProvider(),
                    repositoryInstanceCostCalculator(), costConfig.businessAccountHelper(), entityReservedInstanceMappingStore(),
                    accountRIMappingStore(), reservedInstanceBoughtStore());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create planReservedInstanceStore bean", e);
        }
    }

    @Bean
    public BuyReservedInstanceStore buyReservedInstanceStore() {
        try {
            return new BuyReservedInstanceStore(dbAccessConfig.dsl(), identityProviderConfig.identityProvider());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create BuyReservedInstanceStore bean", e);
        }
    }

    @Bean
    public EntityReservedInstanceMappingStore entityReservedInstanceMappingStore() {
        try {
            return new EntityReservedInstanceMappingStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create EntityReservedInstanceMappingStore bean", e);
        }
    }

    @Bean
    public AccountRIMappingStore accountRIMappingStore() {
        try {
            return new AccountRIMappingStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create AccountRIMappingStore bean", e);
        }
    }

    @Bean
    public ReservedInstanceUtilizationStore reservedInstanceUtilizationStore() {
        try {
            return new ReservedInstanceUtilizationStore(dbAccessConfig.dsl(),
                    reservedInstanceBoughtStore(), reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                    normalizeCouponSamples, couponNormalizationFactor);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ReservedInstanceUtilizationStore bean", e);
        }
    }

    @Bean
    public ReservedInstanceCoverageStore reservedInstanceCoverageStore() {
        try {
            return new ReservedInstanceCoverageStore(dbAccessConfig.dsl(), normalizeCouponSamples,
                    couponNormalizationFactor);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ReservedInstanceCoverageStore bean", e);
        }
    }

    /**
     * ReservedInstanceBoughtRpcService bean.
     *
     * @return The {@link ReservedInstanceBoughtRpcService}
     */
    @Bean
    public ReservedInstanceBoughtRpcService reservedInstanceBoughtRpcService() {
        return new ReservedInstanceBoughtRpcService(reservedInstanceBoughtStore(),
                entityReservedInstanceMappingStore(), repositoryClientConfig.repositoryClient(),
                supplyChainRpcServiceConfig.supplyChainRpcService(), realtimeTopologyContextId,
                pricingConfig.priceTableStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                BuyReservedInstanceServiceGrpc.newBlockingStub(costClientConfig.costChannel()),
                planReservedInstanceStore(),
                pricingConfig.businessAccountPriceTableKeyStore()
                );
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
                accountRIMappingStore(),
                costComponentGlobalConfig.clock());
    }

    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(costComponentGlobalConfig.clock(), updateRetentionIntervalSeconds,
            TimeUnit.SECONDS, numRetainedMinutes, settingServiceBlockingStub);
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
        try {
            return new ReservedInstanceCoverageUpdate(dbAccessConfig.dsl(),
                    entityReservedInstanceMappingStore(), accountRIMappingStore(),
                    reservedInstanceUtilizationStore(), reservedInstanceCoverageStore(),
                    reservedInstanceCoverageValidatorFactory(),
                    supplementalCoverageAnalysisFactory, costNotificationConfig.costNotificationSender(), riCoverageCacheExpireMinutes);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ReservedInstanceCoverageUpdate bean", e);
        }
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
                        costNotificationConfig.costNotificationSender(),
                        realtimeTopologyContextId);
        marketComponent.addProjectedEntityRiCoverageListener(projectedRICoverageListener);
        repositoryClientConfig.repository().addListener(projectedRICoverageListener);
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
                        cloudTopologyFactory(),
                        projectedCommitmentMappingProcessor);
        marketComponent.addProjectedTopologyListener(projectedEntityTopologyListener);
        return projectedEntityTopologyListener;
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                groupClientConfig.groupMemberRetriever());
    }

    /**
     * PlanProjectedRICoverageAndUtilStore bean.
     * @return The {@link PlanProjectedRICoverageAndUtilStore}
     */
    @Bean
    public PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore() {
        try {
            final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore = new PlanProjectedRICoverageAndUtilStore(dbAccessConfig.dsl(),
                    repositoryServiceClient(), planReservedInstanceService(),
                    reservedInstanceSpecConfig.reservedInstanceSpecStore(), accountRIMappingStore(),
                    persistEntityCostChunkSize);
            return planProjectedRICoverageAndUtilStore;
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PlanProjectedRICoverageAndUtilStore bean", e);
        }
    }

    @Bean
    public ActionContextRIBuyStore actionContextRIBuyStore() {
        try {
            return new ActionContextRIBuyStore(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ActionContextRIBuyStore bean", e);
        }
    }

    @Bean
    public ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory() {
        return new ReservedInstanceCoverageValidatorFactory(
                reservedInstanceBoughtStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore());
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
