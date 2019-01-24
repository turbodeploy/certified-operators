package com.vmturbo.history.stats;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsREST.StatsHistoryServiceController;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.StatSnapshotCreator.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.live.HistoryTimeFrameCalculator;
import com.vmturbo.history.stats.live.LiveStatsReader;
import com.vmturbo.history.stats.live.LiveStatsWriter;
import com.vmturbo.history.stats.live.StatsQueryFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.live.SystemLoadWriter;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.utils.SystemLoadHelper;

/**
 * Spring configuration for Stats RPC service related objects.
 **/
@Configuration
@Import({HistoryDbConfig.class,
    GroupClientConfig.class})
public class StatsConfig {

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Value("${retention.numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds}")
    private int updateRetentionIntervalSeconds;

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
                statSnapshotCreator(), statRecordBuilder(),
                systemLoadReader(), systemLoadWriter());
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
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(Clock.systemUTC(), updateRetentionIntervalSeconds,
            TimeUnit.SECONDS, numRetainedMinutes,
            SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    @Bean
    public HistoryTimeFrameCalculator timeFrameCalculator() {
        return new HistoryTimeFrameCalculator(Clock.systemUTC(), retentionPeriodFetcher());
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
        return new LiveStatsWriter(historyDbConfig.historyDbIO(),
                writeTopologyChunkSize, excludedCommoditiesList());
    }

    @Bean
    public SystemLoadWriter systemLoadWriter() {
        SystemLoadWriter systemLoadWriter = new SystemLoadWriter(historyDbConfig.historyDbIO());
        return systemLoadWriter;
    }

    @Bean
    public SystemLoadReader systemLoadReader() {
        SystemLoadReader systemLoadReader = new SystemLoadReader(historyDbConfig.historyDbIO());
        return systemLoadReader;
    }

    @Bean
    public SystemLoadHelper systemLoadHelper() {
        SystemLoadHelper systemLoadUtils = new SystemLoadHelper(systemLoadReader(), systemLoadWriter());
        return systemLoadUtils;
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
    public ClusterStatsReader clusterStatsReader() {
        return new ClusterStatsReader(historyDbConfig.historyDbIO());
    }

    @Bean
    ClusterStatsWriter clusterStatsWriter() {
        return new ClusterStatsWriter(historyDbConfig.historyDbIO());
    }

}
