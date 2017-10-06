package com.vmturbo.history.market;

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
import com.vmturbo.history.HistoryComponent;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.stats.PriceIndexWriter;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.topology.TopologyListenerConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.priceindex.api.impl.PriceIndexReceiver;

/**
 * Configuration for the PriceIndex Listener for the History component
 **/
@Configuration
@Import({TopologyListenerConfig.class, StatsConfig.class, HistoryApiConfig.class})
public class MarketListenerConfig {

    @Autowired
    private HistoryComponent historyComponent;

    @Value("${marketHost}")
    private String marketHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${writeTopologyChunkSize}")
    private int writeTopologyChunkSize;

    @Autowired
    private TopologyListenerConfig topologyListenerConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig apiConfig;

    @Autowired
    private MarketListenerConfig marketListenerConfig;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    /**
     * Create a listener for both Projected Market and PriceIndex information as created by the
     * Market as a result of Analysis.
     */
    @Bean
    public MarketListener marketListener() {
        return new MarketListener(statsConfig.liveStatsWriter(),
                statsConfig.planStatsWriter(),
                marketListenerConfig.priceIndexWriter(),
                statsConfig.topologySnapshotRegistry(),
                realtimeTopologyContextId,
                apiConfig.statsAvailabilityTracker(),
                statsConfig.projectedStatsStore());
    }

    /**
     * Register the listener for Projected Market Topologies
     * @return
     */
    @Bean
    public MarketComponent marketComponent() {
        final ComponentApiConnectionConfig connectionConfig = ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(marketHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
        final MarketComponent market =
                MarketComponentClient.rpcAndNotification(connectionConfig, projectedTopologyThreadPool());
        market.addProjectedTopologyListener(marketListener());
        return market;
    }

    /**
     * Thread pool for the Topology Listener.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService projectedTopologyThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-projected-listener-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    PriceIndexWriter priceIndexWriter() {
        return new PriceIndexWriter(
                statsConfig.topologySnapshotRegistry(),
                historyComponent.historyDbIO(),
                writeTopologyChunkSize);
    }

    /**
     * Register the listener for PriceIndex information with the Market.
     */
    @Bean
    public PriceIndexReceiver priceIndexReceiver() {
        final ComponentApiConnectionConfig connectionConfig =
                ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(marketHost, httpPort)
                .build();
        final PriceIndexReceiver client = PriceIndexReceiver.rpcAndNotification(
                connectionConfig, priceIndexThreadPool());
        client.setPriceIndexListener(marketListener());
        return client;
    }

    /**
     * Thread pool for the PriceIndex Listeners.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService priceIndexThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-priceIndex-listener-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}
