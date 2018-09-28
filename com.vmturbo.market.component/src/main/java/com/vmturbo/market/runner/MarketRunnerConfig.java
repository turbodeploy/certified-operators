package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.rpc.MarketRpcConfig;
import com.vmturbo.market.runner.AnalysisFactory.DefaultAnalysisFactory;
import com.vmturbo.market.runner.cost.MarketCloudCostDataProvider;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory;
import com.vmturbo.market.runner.cost.MarketPriceTableFactory.DefaultMarketPriceTableFactory;
import com.vmturbo.market.topology.TopologyProcessorConfig;

/**
 * Configuration for market runner in the market component.
 */
@Configuration
@Import({MarketApiConfig.class,
        GroupClientConfig.class,
        CostClientConfig.class,
        TopologyProcessorConfig.class,
        MarketRpcConfig.class})
public class MarketRunnerConfig {

    @Autowired
    private MarketApiConfig apiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private TopologyProcessorConfig topologyProcessorConfig;

    @Autowired
    private MarketRpcConfig marketRpcConfig;

    @Value("${alleviatePressureQuoteFactor}")
    private float alleviatePressureQuoteFactor;

    @Value("${suspensionThrottlingPerCluster}")
    private boolean suspensionThrottlingPerCluster;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService marketRunnerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("market-runner-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public MarketRunner marketRunner() {
        return new MarketRunner(
                marketRunnerThreadPool(),
                apiConfig.marketApi(),
                analysisFactory(),
                marketRpcConfig.marketDebugRpcService());
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public GroupServiceBlockingStub groupServiceClient() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public AnalysisFactory analysisFactory() {
        return new DefaultAnalysisFactory(groupServiceClient(),
                settingServiceClient(),
                marketPriceTableFactory(),
                cloudTopologyFactory(),
                Clock.systemUTC(),
                alleviatePressureQuoteFactor,
                suspensionThrottlingPerCluster);
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(topologyProcessorConfig.topologyProcessor());
    }

    @Bean
    public MarketPriceTableFactory marketPriceTableFactory() {
        return new DefaultMarketPriceTableFactory(marketCloudCostDataProvider(),
                discountApplicatorFactory(), topologyEntityInfoExtractor());
    }

    @Nonnull
    public TopologyEntityInfoExtractor topologyEntityInfoExtractor() {
        return new TopologyEntityInfoExtractor();
    }

    @Nonnull
    public DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory() {
        return DiscountApplicator.newFactory();
    }

    @Bean
    public MarketCloudCostDataProvider marketCloudCostDataProvider() {
        return new MarketCloudCostDataProvider(costClientConfig.costChannel());
    }
}
