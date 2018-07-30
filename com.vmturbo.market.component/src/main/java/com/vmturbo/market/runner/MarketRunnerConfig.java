package com.vmturbo.market.runner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.rpc.MarketRpcConfig;
import com.vmturbo.market.runner.Analysis.AnalysisFactory;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

/**
 * Configuration for market runner in the market component.
 */
@Configuration
@Import({MarketApiConfig.class,
        GroupClientConfig.class,
        MarketRpcConfig.class})
public class MarketRunnerConfig {

    @Autowired
    private MarketApiConfig apiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

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
                groupServiceClient(),
                settingServiceClient(),
                marketRpcConfig.marketDebugRpcService(),
                new MarketRunnerConfigWrapper(alleviatePressureQuoteFactor,
                                              suspensionThrottlingPerCluster));
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
        return new AnalysisFactory();
    }

    public class MarketRunnerConfigWrapper{
        private final float alleviatePressureQuoteFactor;
        private final SuspensionsThrottlingConfig suspensionsThrottlingConfig;
        public MarketRunnerConfigWrapper(float alleviatePressureQuoteFactor,
                                  boolean suspensionThrottlingPerCluster) {
            this.alleviatePressureQuoteFactor = alleviatePressureQuoteFactor;
            this.suspensionsThrottlingConfig = suspensionThrottlingPerCluster
                            ? SuspensionsThrottlingConfig.CLUSTER : SuspensionsThrottlingConfig.DEFAULT;
        }
        public float getAlleviatePressureQuoteFactor() {
            return alleviatePressureQuoteFactor;
        }
        public SuspensionsThrottlingConfig getSuspensionsThrottlingConfig() {
            return suspensionsThrottlingConfig;
        }
    }
}
