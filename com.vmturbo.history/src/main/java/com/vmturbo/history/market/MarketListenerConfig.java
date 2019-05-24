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

import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.history.topology.PlanTopologyEntitiesListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.PlanAnalysisTopologyListener;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.market.component.api.impl.MarketSubscription.Topic;

/**
 * Configuration for the PriceIndex Listener for the History component
 **/
@Configuration
@Import({StatsConfig.class, HistoryApiConfig.class,
        MarketClientConfig.class})
public class MarketListenerConfig {

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${writeTopologyChunkSize}")
    private int writeTopologyChunkSize;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig apiConfig;

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
                priceIndexVisitorFactory(),
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
                MarketSubscription.forTopic(Topic.ProjectedTopologies),
                MarketSubscription.forTopic(Topic.PlanAnalysisTopologies));
        market.addProjectedTopologyListener(marketListener());
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
    DBPriceIndexVisitorFactory priceIndexVisitorFactory() {
        return new DBPriceIndexVisitorFactory(historyDbConfig.historyDbIO(),
                writeTopologyChunkSize);
    }
}
