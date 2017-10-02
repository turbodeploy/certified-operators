package com.vmturbo.market.topology;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.market.runner.MarketRunnerConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import(MarketRunnerConfig.class)
public class TopologyProcessorConfig {

    private static Logger log = LogManager.getLogger();

    @Autowired
    private MarketRunnerConfig marketRunnerConfig;

    @Value("${topologyProcessorHost}")
    private String topologyProcessorHost;

    @Value("${server.port}")
    private int topologyProcessorPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        return new TopologyEntitiesListener(marketRunnerConfig.marketRunner());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService tpClientExecutorService() {
        return Executors.newCachedThreadPool(threadFactory());
    }

    @Bean
    public ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("market-component-tp-api-srv-%d").build();
    }

    @Bean
    public ComponentApiConnectionConfig connectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(topologyProcessorHost, topologyProcessorPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean(destroyMethod = "close")
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor =
                TopologyProcessorClient.rpcAndNotification(connectionConfig(), tpClientExecutorService());
        topologyProcessor.addEntitiesListener(topologyEntitiesListener());
        return topologyProcessor;
    }
}
