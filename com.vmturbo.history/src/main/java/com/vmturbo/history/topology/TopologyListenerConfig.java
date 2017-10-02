package com.vmturbo.history.topology;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Set up a Listener for the Topology Processor feed.
 **/
@Configuration
@Import({HistoryApiConfig.class})
public class TopologyListenerConfig {

    @Value("${topologyProcessorHost}")
    private String topologyProcessorHost;

    @Value("${server.port}")
    private int topologyProcessorPort;

    @Value("${realtimeTopologyContextId}")
    long realtimeTopologyContextId;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        return new TopologyEntitiesListener(
                statsConfig.liveStatsWriter(),
                statsConfig.planStatsWriter(),
                realtimeTopologyContextId,
                historyApiConfig.statsAvailabilityTracker());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor =
                TopologyProcessorClient.rpcAndNotification(connectionConfig(), tpClientExecutorService());
        topologyProcessor.addEntitiesListener(topologyEntitiesListener());
        return topologyProcessor;
    }


    @Bean
    public ComponentApiConnectionConfig connectionConfig() {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(topologyProcessorHost, topologyProcessorPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService tpClientExecutorService() {
        return Executors.newCachedThreadPool(tpThreadFactory());
    }

    @Bean
    public ThreadFactory tpThreadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("history-tp-listener-%d").build();
    }

    public long getRealtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

}
