package com.vmturbo.history.stats;

import java.time.Clock;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsREST.StatsHistoryServiceController;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.StatSnapshotCreator.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.live.FullMarketRatioProcessor.FullMarketRatioProcessorFactory;
import com.vmturbo.history.stats.live.RatioRecordFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.PercentileReader;
import com.vmturbo.history.stats.writers.HistUtilizationWriter;
import com.vmturbo.history.stats.writers.LiveStatsWriter;
import com.vmturbo.history.stats.writers.SystemLoadSnapshot;
import com.vmturbo.history.stats.writers.SystemLoadWriter;
import com.vmturbo.history.utils.SystemLoadHelper;

/**
 * Spring configuration for Stats RPC service related objects.
 **/
@Configuration
@Import({HistoryDbConfig.class, GroupClientConfig.class})
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
    @Value("${statsWritersMaxPoolSize}")
    private int statsWritersMaxPoolSize;

    @Value("${systemLoadRecordsPerChunk}")
    private int systemLoadRecordsPerChunk;
    @Value("${timeToWaitNetworkReadinessMs}")
    private int timeToWaitNetworkReadinessMs;
    @Value("${grpcReadingTimeoutMs}")
    private long grpcReadingTimeoutMs;

    @Bean
    public StatsHistoryRpcService statsRpcService() {
        return new StatsHistoryRpcService(realtimeTopologyContextId, liveStatsReader(),
                planStatsReader(), clusterStatsReader(), clusterStatsWriter(),
                historyDbConfig.historyDbIO(),
                projectedStatsStore(), paginationParamsFactory(),
                statSnapshotCreator(), statRecordBuilder(),
                systemLoadReader(),
                systemLoadRecordsPerChunk,
                percentileReader(), statsWritersPool());
    }

    @Bean
    protected PercentileReader percentileReader() {
        return new PercentileReader(timeToWaitNetworkReadinessMs, grpcReadingTimeoutMs, clock(),
                        historyDbConfig.historyDbIO());
    }

    @Bean
    protected Clock clock() {
        return Clock.systemUTC();
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
        return new RetentionPeriodFetcher(clock(), updateRetentionIntervalSeconds,
            TimeUnit.SECONDS, numRetainedMinutes,
            SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(clock(), retentionPeriodFetcher());
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

    /**
     * Persist information in a Live Topology to the History DB. This includes persisting the
     * entities, and for each entity write the stats, including commodities and entity attributes.
     *
     * @return instance of {@link StatsWriteCoordinator}.
     */
    @Bean
    public StatsWriteCoordinator statsWriteCoordinator() {
        final Collection<IStatsWriter> statsWriters = statsWriters();
        final Collection<IStatsWriter> chunkedTopologyStatsWriters = statsWriters.stream()
                        .filter(w -> !ICompleteTopologyStatsWriter.class.isInstance(w))
                        .collect(Collectors.toSet());
        final Collection<ICompleteTopologyStatsWriter> completeTopologyStatsWriters =
                        statsWriters.stream().filter(ICompleteTopologyStatsWriter.class::isInstance)
                                        .map(ICompleteTopologyStatsWriter.class::cast)
                                        .collect(Collectors.toSet());
        return new StatsWriteCoordinator(statsWritersPool(), chunkedTopologyStatsWriters,
                        completeTopologyStatsWriters, writeTopologyChunkSize);
    }

    @Bean
    public Collection<IStatsWriter> statsWriters() {
        return ImmutableSet.of(systemLoadSnapshot(), histUtilizationWriter(), liveStatsWriter());
    }

    /**
     * {@link IStatsWriter} implementation which is writing live statistics into the database.
     *
     * @return instance of {@link LiveStatsWriter}.
     */
    @Bean
    public IStatsWriter liveStatsWriter() {
        return new LiveStatsWriter(historyDbConfig.historyDbIO(), writeTopologyChunkSize,
                        excludedCommoditiesList());
    }

    /**
     * Thread pool to schedule stats writers tasks.
     *
     * @return thread pool.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService statsWritersPool() {
        final int numberOfThreads = Math.min(statsWritersMaxPoolSize, statsWriters().size());
        return new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                        new ReversedBlockingQueue<>(),
                        new ThreadFactoryBuilder().setNameFormat("Chunked Topology Stats Writer#%d")
                                        .build());
    }

    /**
     * {@link IStatsWriter} implementation saves information about the current shapshot of the
     * system, using the commodities participating in the calculation of system load.
     *
     * @return instance of {@link SystemLoadSnapshot}.
     */
    @Bean
    public IStatsWriter systemLoadSnapshot() {
        return new SystemLoadSnapshot(groupServiceClient(), systemLoadHelper());
    }

    /**
     * {@link IStatsWriter} implementation which is going to save historical utilization information
     * into database.
     *
     * @return instance of {@link HistUtilizationWriter}.
     */
    @Bean
    public IStatsWriter histUtilizationWriter() {
        return new HistUtilizationWriter();
    }

    @Bean
    public GroupServiceBlockingStub groupServiceClient() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
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
    Set<String> excludedCommoditiesList() {
        return ImmutableSet.copyOf(excludedCommodities.toLowerCase().split(" "));
    }

    @Bean
    public LiveStatsReader liveStatsReader() {
        return new LiveStatsReader(historyDbConfig.historyDbIO(),
            timeRangeFactory(),
            statsQueryFactory(),
            fullMarketRatioProcessorFactory(),
            ratioRecordFactory());
    }

    @Bean
    public RatioRecordFactory ratioRecordFactory() {
        return new RatioRecordFactory();
    }

    @Bean
    public FullMarketRatioProcessorFactory fullMarketRatioProcessorFactory() {
        return new FullMarketRatioProcessorFactory(ratioRecordFactory());
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
