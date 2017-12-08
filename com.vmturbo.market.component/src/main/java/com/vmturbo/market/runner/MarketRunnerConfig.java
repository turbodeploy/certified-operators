package com.vmturbo.market.runner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.api.MarketApiConfig;
import com.vmturbo.market.runner.Analysis.AnalysisFactory;

/**
 * Configuration for market runner in the market component.
 */
@Configuration
@Import({MarketApiConfig.class,
        GroupClientConfig.class})
public class MarketRunnerConfig {

    @Autowired
    private MarketApiConfig apiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

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
                settingServiceClient());
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public AnalysisFactory analysisFactory() {
        return new AnalysisFactory();
    }
}
