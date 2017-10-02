package com.vmturbo.history.stats;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.stats.StatsREST.StatsHistoryServiceController;
import com.vmturbo.history.HistoryComponent;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.topology.TopologySnapshotRegistry;

/**
 * Spring configuration for Stats RPC service related objects.
 **/
@Configuration
public class StatsConfig {

    @Autowired
    private HistoryComponent historyComponent;

    @Value("${numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${numRetainedHours}")
    private int numRetainedHours;

    @Value("${numRetainedDays}")
    private int numRetainedDays;

    @Value("${writeTopologyChunkSize}")
    private int writeTopologyChunkSize;

    @Value("${excludedCommodities}")
    private String excludedCommodities;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public StatsHistoryService statsRpcService() {
        return new StatsHistoryService(realtimeTopologyContextId, liveStatsReader(),
                planStatsReader(), clusterStatsWriter(), historyComponent.historyDbIO(),
                projectedStatsStore());
    }

    @Bean
    public ProjectedStatsStore projectedStatsStore() {
        return new ProjectedStatsStore();
    }

    @Bean
    public LiveStatsWriter liveStatsWriter() {
        return new LiveStatsWriter(topologySnapshotRegistry(), historyComponent.historyDbIO(),
                writeTopologyChunkSize, excludedCommoditiesList());
    }

    @Bean
    ImmutableList<String> excludedCommoditiesList() {
        return ImmutableList.copyOf(excludedCommodities.toLowerCase()
                .split(" "));
    }

    @Bean
    public LiveStatsReader liveStatsReader() {
        return new LiveStatsReader(historyComponent.historyDbIO(), numRetainedMinutes,
                numRetainedHours, numRetainedDays);
    }

    @Bean
    public StatsHistoryServiceController statsRestController() {
        return new StatsHistoryServiceController(statsRpcService());
    }

    @Bean
    public PlanStatsWriter planStatsWriter() {
        return new PlanStatsWriter(historyComponent.historyDbIO());
    }

    @Bean
    public PlanStatsReader planStatsReader() {
        return new PlanStatsReader(historyComponent.historyDbIO());
    }

    @Bean
    public TopologySnapshotRegistry topologySnapshotRegistry() {
        return new TopologySnapshotRegistry();
    }

    @Bean
    ClusterStatsWriter clusterStatsWriter() {
        return new ClusterStatsWriter(historyComponent.historyDbIO());
    }

}
