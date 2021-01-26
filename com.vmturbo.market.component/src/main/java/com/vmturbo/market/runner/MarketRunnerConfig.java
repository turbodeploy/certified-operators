package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.api.impl.CostSubscription;
import com.vmturbo.cost.api.impl.CostSubscription.Topic;
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
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.AnalysisRICoverageListener;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.diagnostics.MarketComponentDiagnosticsHandler;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisConfig;
import com.vmturbo.market.rpc.MarketRpcConfig;
import com.vmturbo.market.runner.AnalysisFactory.DefaultAnalysisFactory;
import com.vmturbo.market.runner.cost.MarketCloudCostDataProvider;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory.DefaultMarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisService;
import com.vmturbo.market.runner.cost.MigratedWorkloadCloudCommitmentAnalysisServiceImpl;
import com.vmturbo.market.runner.wastedfiles.WastedFilesAnalysisEngine;
import com.vmturbo.market.topology.TopologyProcessorConfig;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.ReversibilitySettingFetcherFactory.DefaultReversibilitySettingFetcherFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory.DefaultTierExcluderFactory;
import com.vmturbo.topology.processor.api.util.ConcurrentLimitProcessingGate;
import com.vmturbo.topology.processor.api.util.SingleTopologyProcessingGate;
import com.vmturbo.topology.processor.api.util.TopologyProcessingGate;

/**
 * Configuration for market runner in the market component.
 */
@Configuration
@Import({MarketApiConfig.class,
        GroupClientConfig.class,
        CostClientConfig.class,
        MarketRpcConfig.class,
        BuyRIImpactAnalysisConfig.class})
public class MarketRunnerConfig {

    @Autowired
    private MarketApiConfig apiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    /**
     * No associated @Import because of the circular dependency between {@link TopologyProcessorConfig}
     * and {@link MarketRunnerConfig}.
     */
    @SuppressWarnings("unused")
    @Autowired
    private TopologyProcessorConfig topologyProcessorConfig;

    @Autowired
    private MarketRpcConfig marketRpcConfig;

    @Autowired
    private BuyRIImpactAnalysisConfig buyRIImpactAnalysisConfig;

    @Value("${alleviatePressureQuoteFactor:0.2}")
    private float alleviatePressureQuoteFactor;

    @Value("${standardQuoteFactor:0.68}")
    private float standardQuoteFactor;

    @Value("${marketMode:M2Only}")
    private String marketMode;

    @Value("${suspensionThrottlingPerCluster:true}")
    private boolean suspensionThrottlingPerCluster;

    // The plan market and cloud entity move cost factor is currently always 0
    @Value("${liveMarketMoveCostFactor:0.05}")
    private float liveMarketMoveCostFactor;

    @Value("${concurrentPlanAnalyses:1}")
    private int concurrentPlanAnalyses;

    @Value("${analysisQueueTimeoutMins:90}")
    private long analysisQueueTimeoutMins;

    /**
     * The type of {@link TopologyProcessingGate} to use.
     */
    @Value("${topologyProcessingGateType:concurrent}")
    private String topologyProcessingGateType;

    @Value("${fullPriceForQuote:false}")
    private boolean fullPriceForQuote;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService marketRunnerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-runner-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /**
     * Utility to restrict the number of concurrent analyses we run.
     *
     * @return The {@link TopologyProcessingGate}.
     */
    @Bean
    public TopologyProcessingGate analysisGate() {
        switch (topologyProcessingGateType) {
            case "single":
                return new SingleTopologyProcessingGate(analysisQueueTimeoutMins, TimeUnit.MINUTES);
            case "concurrent":
            default:
                // In the future we could use a configuration property to control what kind of gate to use.
                return new ConcurrentLimitProcessingGate(concurrentPlanAnalyses,
                        analysisQueueTimeoutMins, TimeUnit.MINUTES);
        }
    }

    @Bean
    public MarketRunner marketRunner() {
        return new MarketRunner(
            marketRunnerThreadPool(),
            apiConfig.marketApi(),
            analysisFactory(),
            marketRpcConfig.marketDebugRpcService(),
            analysisGate(),
            marketRpcConfig.getInitialPlacementFinder());
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public GroupMemberRetriever groupMemberRetriever() {
        return new GroupMemberRetriever(GroupServiceGrpc.newBlockingStub(
                groupClientConfig.groupChannel()));
    }

    @Bean
    public AnalysisFactory analysisFactory() {
        return new DefaultAnalysisFactory(groupMemberRetriever(),
                settingServiceClient(),
                marketPriceTableFactory(),
                cloudTopologyFactory(),
                topologyCostCalculatorFactory(),
                wastedFilesAnalysisFactory(),
                buyRIImpactAnalysisConfig.buyRIImpactAnalysisFactory(),
                marketCloudCostDataProvider(),
                Clock.systemUTC(),
                alleviatePressureQuoteFactor,
                standardQuoteFactor,
                marketMode,
                liveMarketMoveCostFactor,
                suspensionThrottlingPerCluster,
                tierExcluderFactory(),
                analysisRICoverageListener(),
                consistentResizerFactory(),
                reversibilitySettingFetcherFactory(),
                migratedWorkloadCloudCommitmentAnalysisService(),
                fullPriceForQuote,
                commodityIdUpdater());
    }

    /**
     * Create {@link CommodityIdUpdater}.
     *
     * @return {@link CommodityIdUpdater}
     */
    @Bean
    public CommodityIdUpdater commodityIdUpdater() {
        return new CommodityIdUpdater();
    }

    /**
     * creates a handler for analysis diagnostics collection.
     * @return a new MarketComponentDiagnosticsHandler
     */
    @Bean
    public MarketComponentDiagnosticsHandler diagsHandler() {
        return new MarketComponentDiagnosticsHandler();
    }

    /**
     * Creates a new settingPolicyServiceBlockingStub which can be used to interact with setting
     * policy rpc service in group-component.
     *
     * @return a new SettingPolicyServiceBlockingStub
     */
    @Bean
    public SettingPolicyServiceBlockingStub settingPolicyRpcService() {
        return SettingPolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Creates a new {@link TierExcluderFactory}.
     *
     * @return a new {@link TierExcluderFactory}
     */
    @Bean
    public TierExcluderFactory tierExcluderFactory() {
        return new DefaultTierExcluderFactory(settingPolicyRpcService());
    }

    @Bean
    public WastedFilesAnalysisEngine wastedFilesAnalysisFactory() {
        return new WastedFilesAnalysisEngine();
    }

    /**
     * Get the instance of the topologyCostCalculator factory.
     *
     * @return The topology cost calculator factory.
     */
    @Bean
    public TopologyCostCalculatorFactory topologyCostCalculatorFactory() {
        return new DefaultTopologyCostCalculatorFactory(topologyEntityInfoExtractor(),
                cloudCostCalculatorFactory(),
                marketCloudCostDataProvider(),
                discountApplicatorFactory(),
                riApplicatorFactory());
    }

    @Bean
    public ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory() {
        return ReservedInstanceApplicator.newFactory();
    }

    @Bean
    public CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory() {
        return CloudCostCalculator.newFactory();
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                new GroupMemberRetriever(GroupServiceGrpc
                        .newBlockingStub(groupClientConfig.groupChannel())));
    }

    @Bean
    public MarketPriceTableFactory marketPriceTableFactory() {
        return new DefaultMarketPriceTableFactory(discountApplicatorFactory(), topologyEntityInfoExtractor());
    }

    @Nonnull
    public TopologyEntityInfoExtractor topologyEntityInfoExtractor() {
        return new TopologyEntityInfoExtractor();
    }

    @Nonnull
    public DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory() {
        return DiscountApplicator.newFactory();
    }

    /**
     * Get the market cloud cost data provider.
     *
     * @return The market cloud cost data provider.
     */
    @Bean
    public MarketCloudCostDataProvider marketCloudCostDataProvider() {
        return new MarketCloudCostDataProvider(costClientConfig.costChannel(), discountApplicatorFactory(),
                topologyEntityInfoExtractor());
    }

    /**
     * Factory method for creating AnalysisRICoverageListener instances. The created instance is
     * registered with the CostComponent to listen to the Cost status notification topic.
     *
     * @return an instance of AnalysisRICoverageListener.
     */
    @Bean
    public AnalysisRICoverageListener analysisRICoverageListener() {
        final AnalysisRICoverageListener listener = new AnalysisRICoverageListener();
        costClientConfig.costComponent(CostSubscription.forTopic(Topic.COST_STATUS_NOTIFICATION))
                .addCostNotificationListener(listener);
        return listener;
    }

    @Bean
    public MigratedWorkloadCloudCommitmentAnalysisService migratedWorkloadCloudCommitmentAnalysisService() {
        return new MigratedWorkloadCloudCommitmentAnalysisServiceImpl(costClientConfig.costChannel());
    }

    /**
     * Creates a new {@link ConsistentScalingHelperFactory}.
     * @return a new {@link ConsistentScalingHelperFactory}
     */
    @Nonnull
    public ConsistentScalingHelperFactory consistentResizerFactory() {
        return new ConsistentScalingHelperFactory(settingPolicyRpcService());
    }

    /**
     * Creates a new {@link ReversibilitySettingFetcherFactory}.
     *
     * @return a new {@link ReversibilitySettingFetcherFactory}
     */
    @Bean
    public ReversibilitySettingFetcherFactory reversibilitySettingFetcherFactory() {
        return new DefaultReversibilitySettingFetcherFactory(settingPolicyRpcService());
    }
}
