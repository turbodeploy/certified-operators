package com.vmturbo.cost.component.topology;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory.DefaultTopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.SupplyChainServiceConfig;
import com.vmturbo.cost.component.TopologyProcessorListenerConfig;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.cloud.commitment.TopologyCommitmentConfig;
import com.vmturbo.cost.component.cloud.commitment.TopologyCommitmentCoverageEstimator.CommitmentCoverageEstimatorFactory;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.discount.DiscountConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.entity.cost.RollupEntityCostProcessor;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceRollupProcessor;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.rollup.RollupConfig;
import com.vmturbo.cost.component.savings.EntitySavingsConfig;
import com.vmturbo.cost.component.savings.EntitySavingsTopologyMonitor;
import com.vmturbo.cost.component.topology.cloud.listener.CCADemandCollector;
import com.vmturbo.cost.component.topology.cloud.listener.EntityCostWriter;
import com.vmturbo.cost.component.topology.cloud.listener.LiveCloudTopologyListener;
import com.vmturbo.cost.component.topology.cloud.listener.RIBuyRunner;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
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
        DbAccessConfig.class,
        SupplyChainServiceConfig.class,
        GroupClientConfig.class,
        CloudCommitmentAnalysisStoreConfig.class,
        CostClientConfig.class,
        CostComponentGlobalConfig.class,
        RollupConfig.class,
        TopologyCommitmentConfig.class
})
public class TopologyListenerConfig {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private RollupConfig rollupConfig;

    @Autowired
    private CostComponentGlobalConfig costComponentGlobalConfig;

    @Autowired
    private TopologyProcessorListenerConfig topologyProcessorListenerConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    @Autowired
    private EntitySavingsConfig entitySavingsConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${maxTrackedLiveTopologies:10}")
    private int maxTrackedLiveTopologies;

    @Value("${liveTopology.cleanupInterval:PT1H}")
    private String liveTopologyCleanupInterval;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener(
            @Nonnull EntityCostWriter entityCostWriter) {
        List<LiveCloudTopologyListener> cloudTopologyListenerList =
                new ArrayList<>(Arrays.asList(entityCostWriter, riBuyRunner(), ccaDemandCollector()));
        if (FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()) {
                cloudTopologyListenerList.add(entitySavingsTopologyMonitor());
        }
        final LiveTopologyEntitiesListener entitiesListener =
                new LiveTopologyEntitiesListener(
                        cloudTopologyFactory(),
                        reservedInstanceConfig.reservedInstanceCoverageUpload(),
                        costConfig.businessAccountHelper(),
                        topologyProcessorListenerConfig.liveTopologyInfoTracker(),
                        ingestedTopologyStore(),
                        cloudTopologyListenerList);

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
        try {
            return new IngestedTopologyStore(liveTopologyCleanupScheduler(), Duration.parse(liveTopologyCleanupInterval),
                    dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create IngestedTopologyStore bean", e);
        }
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
        try {
            return new PriceTableKeyIdentityStore(dbAccessConfig.dsl(), identityProviderConfig.identityProvider());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PriceTableKeyIdentityStore bean", e);
        }
    }

    /**
     * Get the business account price table key store.
     *
     * @return the business account price table key store.
     */
    @Bean
    public BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore() {
        try {
            return new BusinessAccountPriceTableKeyStore(dbAccessConfig.dsl(),
                    priceTableKeyIdentityStore());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create BusinessAccountPriceTableKeyStore bean", e);
        }
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
    public EntityCostWriter entityCostWriter(
            // Imported from TopologyCommitmentConfig
            @Nonnull CommitmentCoverageEstimatorFactory commitmentCoverageEstimatorFactory) {

        return new EntityCostWriter(reservedInstanceConfig.reservedInstanceCoverageUpload(), topologyCostCalculatorFactory(),
                costJournalRecorder(), entityCostConfig.entityCostStore(),
                costNotificationConfig.costNotificationSender(),
                commitmentCoverageEstimatorFactory);
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

    /**
     * Get instance of rollup processor.
     *
     * @return Rollup processor.
     */
    @Bean
    @Conditional(DbEndpointCondition.class)
    public RollupEntityCostProcessor rollupEntityCostProcessor() {
        RollupEntityCostProcessor rollupEntityCostProcessor = new RollupEntityCostProcessor(
                entityCostConfig.entityCostStore(), rollupConfig.entityCostRollupTimesStore(),
                ingestedTopologyStore(), costComponentGlobalConfig.clock());

        int initialDelayMinutes = 1;
        entityCostRollUpExpirationScheduledExecutor().scheduleAtFixedRate(
                rollupEntityCostProcessor::execute, initialDelayMinutes, 60, TimeUnit.MINUTES);
        logger.info("EntityCostProcessor is enabled, will run every hour after 1 min.");
        return rollupEntityCostProcessor;
    }

    /**
     * Setup and return a ScheduledExecutorService for the running of recurrent tasks.
     *
     * @return a new single threaded scheduled executor service with the thread factory configured.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService entityCostRollUpExpirationScheduledExecutor() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "Entity-Cost-Rollup").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    /**
     * Bean for the Entity savings topology monitor.
     *
     * @return An instance of the entity savings topology monitor.
     */
    @Bean
    public EntitySavingsTopologyMonitor entitySavingsTopologyMonitor() {
        return new EntitySavingsTopologyMonitor(entitySavingsConfig.topologyEventsMonitor(),
                entitySavingsConfig.entityStateStore(), entitySavingsConfig.entityEventsJournal());
    }

    /**
     * Get instance of rollup processor.
     *
     * @return Rollup processor.
     */
    @Bean
    @Conditional(DbEndpointCondition.class)
    public ReservedInstanceRollupProcessor reservedInstanceRollupProcessor() {
        ReservedInstanceRollupProcessor reservedInstanceRollupProcessor = new ReservedInstanceRollupProcessor(
            reservedInstanceConfig.reservedInstanceUtilizationStore(),
            reservedInstanceConfig.reservedInstanceCoverageStore(),
            rollupConfig.reservedInstanceUtilizationRollupTimesStore(),
            rollupConfig.reservedInstanceCoverageRollupTimesStore(),
            ingestedTopologyStore(), costComponentGlobalConfig.clock());
        rIUtilizationScheduledExecutor().scheduleAtFixedRate(
            reservedInstanceRollupProcessor::execute, 0, 60, TimeUnit.MINUTES);
        final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        logger.info("ReservedInstanceRollupProcessor is enabled, will run at hour+{} min.",
            sdf3.format(new Timestamp(System.currentTimeMillis())));
        return reservedInstanceRollupProcessor;
    }

    /**
     * Setup and return a ScheduledExecutorService for the running of recurrent tasks.
     *
     * @return a new single threaded scheduled executor service with the thread factory configured.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService rIUtilizationScheduledExecutor() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
            "Ri-Coverage-Utilization-Rollup").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }
}
