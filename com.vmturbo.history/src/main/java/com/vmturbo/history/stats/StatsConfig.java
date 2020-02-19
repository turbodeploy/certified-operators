package com.vmturbo.history.stats;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jooq.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.Builder;
import com.vmturbo.common.protobuf.stats.StatsREST.StatsHistoryServiceController;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.live.FullMarketRatioProcessor.FullMarketRatioProcessorFactory;
import com.vmturbo.history.stats.live.RatioRecordFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.stats.readers.HistUtilizationReader;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.PercentileReader;
import com.vmturbo.history.stats.snapshots.CapacityRecordVisitor.CapacityPopulator;
import com.vmturbo.history.stats.snapshots.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProducerIdPopulator;
import com.vmturbo.history.stats.snapshots.PropertyTypeVisitor.PropertyTypePopulator;
import com.vmturbo.history.stats.snapshots.SharedPropertyPopulator;
import com.vmturbo.history.stats.snapshots.StatSnapshotCreator;
import com.vmturbo.history.stats.snapshots.UsageRecordVisitor.UsagePopulator;

/**
 * Spring configuration for Stats RPC service related objects.
 **/
@Configuration
@Import({HistoryDbConfig.class, GroupClientConfig.class})
public class StatsConfig {
    /**
     * Populates related entity type into {@link StatRecord.Builder} instance.
     */
    public static final SharedPropertyPopulator<String> RELATED_ENTITY_TYPE_POPULATOR =
                    new SharedPropertyPopulator<String>() {
                        @Override
                        public void accept(@Nonnull StatRecord.Builder builder, @Nullable String value,
                                        @Nullable Record record) {
                            if (value != null) {
                                builder.setRelatedEntityType(value);
                            }
                        }
                    };
    /**
     * Populates relation into {@link StatRecord.Builder} instance.
     */
    public static final SharedPropertyPopulator<String> RELATION_POPULATOR =
                    new SharedPropertyPopulator<String>() {
                        @Override
                        public void accept(@Nonnull Builder builder, @Nullable String value,
                                        @Nullable Record record) {
                            if (value != null) {
                                builder.setRelation(value);
                            }
                        }
                    };
    /**
     * Populates property type related information into {@link StatRecord.Builder} instance.
     */
    public static final PropertyTypePopulator PROPERTY_TYPE_POPULATOR =
                    new PropertyTypePopulator();
    /**
     * Populates usage related information into {@link StatRecord.Builder} instance.
     */
    public static final UsagePopulator USAGE_POPULATOR = new UsagePopulator();
    /**
     * Populates capacity related information into {@link StatRecord.Builder} instance.
     */
    public static final CapacityPopulator CAPACITY_POPULATOR = new CapacityPopulator();


    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    HistoryApiConfig historyApiConfig;

    @Value("${retention.numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds}")
    private int updateRetentionIntervalSeconds;

    @Value("${latestTableTimeWindowMin}")
    private int latestTableTimeWindowMin;

    @Value("${realtimeTopologyContextId}")
    public long realtimeTopologyContextId;

    @Value("${historyPaginationDefaultLimit}")
    private int historyPaginationDefaultLimit;

    @Value("${historyPaginationMaxLimit}")
    private int historyPaginationMaxLimit;

    @Value("${historyPaginationDefaultSortCommodity}")
    private String historyPaginationDefaultSortCommodity;

    @Value("${systemLoadRecordsPerChunk}")
    private int systemLoadRecordsPerChunk;
    @Value("${timeToWaitNetworkReadinessMs}")
    private int timeToWaitNetworkReadinessMs;
    @Value("${grpcReadingTimeoutMs}")
    private long grpcReadingTimeoutMs;
    @Value("${history.entitiesReadPerChunk:5000}")
    private int entitiesReadPerChunk;


    @Bean
    public StatsHistoryRpcService statsRpcService() {
        return new StatsHistoryRpcService(
                realtimeTopologyContextId,
                liveStatsReader(),
                planStatsReader(),
                clusterStatsReader(),
                clusterStatsWriter(),
                historyDbConfig.historyDbIO(),
                projectedStatsStore(),
                paginationParamsFactory(),
                statSnapshotCreator(),
                statRecordBuilder(),
                systemLoadReader(),
                systemLoadRecordsPerChunk,
                percentileReader(),
                statsSvcThreadPool());
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
        return createStatSnapshotCreator(liveStatsReader());
    }

    protected static DefaultStatSnapshotCreator createStatSnapshotCreator(
                    LiveStatsReader liveStatsReader) {
        return new DefaultStatSnapshotCreator(new ProducerIdPopulator(liveStatsReader));
    }

    @Bean
    public StatRecordBuilder statRecordBuilder() {
        return createStatRecordBuilder(liveStatsReader());
    }

    protected static DefaultStatRecordBuilder createStatRecordBuilder(
                    LiveStatsReader liveStatsReader) {
        return new DefaultStatRecordBuilder(RELATED_ENTITY_TYPE_POPULATOR, RELATION_POPULATOR,
                        USAGE_POPULATOR, CAPACITY_POPULATOR, PROPERTY_TYPE_POPULATOR,
                        new ProducerIdPopulator(liveStatsReader));
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

    @Bean
    public SystemLoadReader systemLoadReader() {
        SystemLoadReader systemLoadReader = new SystemLoadReader(historyDbConfig.historyDbIO());
        return systemLoadReader;
    }

    @Bean
    public LiveStatsReader liveStatsReader() {
        return new LiveStatsReader(historyDbConfig.historyDbIO(),
            timeRangeFactory(),
            statsQueryFactory(),
            fullMarketRatioProcessorFactory(),
            ratioRecordFactory(),
            histUtilizationReader(),
            entitiesReadPerChunk);
    }

    @Bean
    protected HistUtilizationReader histUtilizationReader() {
        return new HistUtilizationReader(historyDbConfig.historyDbIO(), entitiesReadPerChunk);
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

    @Bean
    ExecutorService statsSvcThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("stats-hist-svc-pool-%d")
                .build();
        return Executors.newCachedThreadPool(threadFactory);
    }

}
