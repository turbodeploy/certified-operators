package com.vmturbo.history.market;

import java.util.EnumSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.stats.PriceIndexWriter;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.topology.PlanTopologyEntitiesListener;
import com.vmturbo.history.topology.TopologyListenerConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.PlanAnalysisTopologyListener;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig.Subscription;

/**
 * Configuration for the PriceIndex Listener for the History component
 **/
@Configuration
@Import({TopologyListenerConfig.class, StatsConfig.class, HistoryApiConfig.class,
        MarketClientConfig.class})
public class MarketListenerConfig {

    @Autowired
    private HistoryDbConfig historyDbConfig;

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

    @Autowired
    private MarketClientConfig marketClientConfig;

    /**
     * Create a listener for both Projected Market and PriceIndex information as created by the
     * Market as a result of Analysis.
     */
    @Bean
    public MarketListener marketListener() {
        return new MarketListener(
                statsConfig.planStatsWriter(),
                marketListenerConfig.priceIndexWriter(),
                realtimeTopologyContextId,
                apiConfig.statsAvailabilityTracker(),
                statsConfig.projectedStatsStore());
    }

    /**
     * Create a listener for plan analysis topologies.
     * @return the plan analysis topology listener
     */
    @Bean
    public PlanAnalysisTopologyListener planAnalysisTopologyListener() {
        return new PlanTopologyEntitiesListener(
                statsConfig.planStatsWriter(),
                apiConfig.statsAvailabilityTracker());
    }

    /**
     * Create a market component listener instance.
     * @return A configured market component listener.
     */
    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
                EnumSet.of(Subscription.ProjectedTopologies, Subscription.PriceIndexes,
                        Subscription.PlanAnalysisTopologies));
        market.addProjectedTopologyListener(marketListener());
        market.addPriceIndexListener(marketListener());
        market.addPlanAnalysisTopologyListener(planAnalysisTopologyListener());
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
                historyDbConfig.historyDbIO(),
                writeTopologyChunkSize);
    }
}
