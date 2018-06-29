package com.vmturbo.history.stats;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.stats.StatsREST.StatsHistoryServiceController;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.StatSnapshotCreator.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.live.LiveStatsReader;
import com.vmturbo.history.stats.live.LiveStatsWriter;
import com.vmturbo.history.stats.live.StatsQueryFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;
import com.vmturbo.history.stats.live.TimeFrameCalculator;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.topology.TopologySnapshotRegistry;

/**
 * Spring configuration for Stats RPC service related objects.
 **/
@Configuration
@Import({HistoryDbConfig.class})
public class StatsConfig {

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Value("${numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${numRetainedHours}")
    private int numRetainedHours;

    @Value("${numRetainedDays}")
    private int numRetainedDays;

    @Value("${latestTableTimeWindowMin}")
    private int latestTableTimeWindowMin;

    @Value("${writeTopologyChunkSize}")
    private int writeTopologyChunkSize;

    @Value("${excludedCommodities}")
    private String excludedCommodities;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${historyPaginationDefaultLimit}")
    private int historyPaginationDefaultLimit;

    @Value("${historyPaginationMaxLimit}")
    private int historyPaginationMaxLimit;

    @Value("${historyPaginationDefaultSortCommodity}")
    private String historyPaginationDefaultSortCommodity;

    @Bean
    public StatsHistoryRpcService statsRpcService() {
        return new StatsHistoryRpcService(realtimeTopologyContextId, liveStatsReader(),
                planStatsReader(), clusterStatsReader(), clusterStatsWriter(),
                historyDbConfig.historyDbIO(),
                projectedStatsStore(), paginationParamsFactory(),
                statSnapshotCreator(), statRecordBuilder());
    }

    @Bean
    public StatSnapshotCreator statSnapshotCreator() {
        return new DefaultStatSnapshotCreator(statRecordBuilder());
    }

    @Bean
    public StatRecordBuilder statRecordBuilder() {
        return new DefaultStatRecordBuilder(liveStatsReader());
    }

    @Bean
    public StatsQueryFactory statsQueryFactory() {
        return new DefaultStatsQueryFactory(historyDbConfig.historyDbIO());
    }

    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(Clock.systemUTC(), numRetainedMinutes, numRetainedHours, numRetainedDays);
    }

    @Bean
    public TimeRangeFactory timeRangeFactory() {
        return new DefaultTimeRangeFactory(historyDbConfig.historyDbIO(),
                timeFrameCalculator(),
                latestTableTimeWindowMin, TimeUnit.MINUTES);
    }

    @Bean
    public EntityStatsPaginationParamsFactory paginationParamsFactory() {
        return new DefaultEntityStatsPaginationParamsFactory(historyPaginationDefaultLimit,
                historyPaginationMaxLimit, historyPaginationDefaultSortCommodity);
    }

    @Bean
    public ProjectedStatsStore projectedStatsStore() {
        return new ProjectedStatsStore();
    }

    @Bean
    public LiveStatsWriter liveStatsWriter() {
        return new LiveStatsWriter(topologySnapshotRegistry(), historyDbConfig.historyDbIO(),
                writeTopologyChunkSize, excludedCommoditiesList());
    }

    @Bean
    ImmutableList<String> excludedCommoditiesList() {
        return ImmutableList.copyOf(excludedCommodities.toLowerCase()
                .split(" "));
    }

    @Bean
    public LiveStatsReader liveStatsReader() {
        return new LiveStatsReader(historyDbConfig.historyDbIO(), timeRangeFactory(), statsQueryFactory());
    }

    @Bean
    public StatsHistoryServiceController statsRestController() {
        return new StatsHistoryServiceController(statsRpcService());
    }

    @Bean
    public PlanStatsWriter planStatsWriter() {
        return new PlanStatsWriter(historyDbConfig.historyDbIO());
    }

    @Bean
    public PlanStatsReader planStatsReader() {
        return new PlanStatsReader(historyDbConfig.historyDbIO());
    }

    @Bean
    public TopologySnapshotRegistry topologySnapshotRegistry() {
        return new TopologySnapshotRegistry();
    }

    @Bean
    public ClusterStatsReader clusterStatsReader() {
        return new ClusterStatsReader(historyDbConfig.historyDbIO());
    }

    @Bean
    ClusterStatsWriter clusterStatsWriter() {
        return new ClusterStatsWriter(historyDbConfig.historyDbIO());
    }

}
