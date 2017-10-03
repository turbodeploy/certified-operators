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
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Set up a Listener for the Topology Processor feed.
 **/
@Configuration
@Import({HistoryApiConfig.class, TopologyProcessorClientConfig.class})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

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
        final TopologyProcessor topologyProcessor = topologyClientConfig.topologyProcessor();
        topologyProcessor.addEntitiesListener(topologyEntitiesListener());
        return topologyProcessor;
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
