package com.vmturbo.history.stats;

import java.sql.SQLException;
import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Record;
import org.springframework.beans.factory.BeanCreationException;
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
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.db.DbAccessConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.listeners.HistoryPlanGarbageCollector;
import com.vmturbo.history.schema.abstraction.tables.ClusterStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.records.ClusterStatsByDayRecord;
import com.vmturbo.history.stats.StatRecordBuilder.DefaultStatRecordBuilder;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor;
import com.vmturbo.history.stats.live.ComputedPropertiesProcessor.ComputedPropertiesProcessorFactory;
import com.vmturbo.history.stats.live.LiveStatsStore;
import com.vmturbo.history.stats.live.StatsQueryFactory;
import com.vmturbo.history.stats.live.StatsQueryFactory.DefaultStatsQueryFactory;
import com.vmturbo.history.stats.live.SystemLoadReader;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.ClusterTimeRangeFactory;
import com.vmturbo.history.stats.live.TimeRange.TimeRangeFactory.DefaultTimeRangeFactory;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.stats.readers.HistUtilizationReader;
import com.vmturbo.history.stats.readers.LiveStatsReader;
import com.vmturbo.history.stats.readers.MovingStatisticsReader;
import com.vmturbo.history.stats.readers.PercentileReader;
import com.vmturbo.history.stats.readers.VolumeAttachmentHistoryReader;
import com.vmturbo.history.stats.snapshots.CapacityRecordVisitor.CapacityPopulator;
import com.vmturbo.history.stats.snapshots.DefaultStatSnapshotCreator;
import com.vmturbo.history.stats.snapshots.ProducerIdVisitor.ProducerIdPopulator;
import com.vmturbo.history.stats.snapshots.PropertyTypeVisitor.PropertyTypePopulator;
import com.vmturbo.history.stats.snapshots.SharedPropertyPopulator;
import com.vmturbo.history.stats.snapshots.StatSnapshotCreator;
import com.vmturbo.history.stats.snapshots.UsageRecordVisitor.UsagePopulator;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Spring configuration for Stats RPC service related objects.
 **/
@Configuration
@Import({HistoryDbConfig.class,
        IngestersConfig.class,
    GroupClientConfig.class,
    PlanOrchestratorClientConfig.class})
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
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private IngestersConfig ingestersConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Value("${retention.numRetainedMinutes:130}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds:10}")
    private int updateRetentionIntervalSeconds;

    @Value("${latestTableTimeWindowMin:15}")
    private int latestTableTimeWindowMin;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${historyPaginationDefaultLimit:100}")
    private int historyPaginationDefaultLimit;

    @Value("${historyPaginationMaxLimit:500}")
    private int historyPaginationMaxLimit;

    @Value("${historyPaginationDefaultSortCommodity:priceIndex}")
    private String historyPaginationDefaultSortCommodity;

    @Value("${maxAmountOfEntitiesPerGrpcMessage:200}")
    private int maxAmountOfEntitiesPerGrpcMessage;

    @Value("${systemLoadRecordsPerChunk:500}")
    private int systemLoadRecordsPerChunk;
    @Value("${timeToWaitNetworkReadinessMs:10}")
    private int timeToWaitNetworkReadinessMs;
    @Value("${grpcReadingTimeoutMs:300000}")
    private long grpcReadingTimeoutMs;
    @Value("${history.entitiesReadPerChunk:5000}")
    private int entitiesReadPerChunk;

    /**
     * Bulk loader for `cluster_stats_by_day` table, used to save headroom stats.
     *
     * @return bulk loader
     */
    @Bean
    public BulkLoader<ClusterStatsByDayRecord> clusterStatsByDayLoader() {
        final SimpleBulkLoaderFactory loaders;
        try {
            loaders = new SimpleBulkLoaderFactory(dbAccessConfig.dsl(),
                    dbAccessConfig.bulkLoaderConfig(),
                    ingestersConfig.partitionManager(), () -> Executors.newSingleThreadExecutor());
            return loaders.getLoader(ClusterStatsByDay.CLUSTER_STATS_BY_DAY);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create clusterStatsByDayLoader bean", e);
        }
    }

    /**
     * Cleans up stale plan data in the history component.
     *
     * @return The {@link PlanGarbageDetector}.
     */
    @Bean
    public PlanGarbageDetector historyPlanGarbageDetector() {
        HistoryPlanGarbageCollector listener = new HistoryPlanGarbageCollector(
                dbAccessConfig.historyDbIO());
        return planOrchestratorClientConfig.newPlanGarbageDetector(listener);
    }

    /**
     * Create {@link StatsHistoryRpcService} for history.
     *
     * @return StatsHistoryRpcService
     */
    @Bean
    public StatsHistoryRpcService statsRpcService() {
        try {
            return new StatsHistoryRpcService(
                    realtimeTopologyContextId,
                    liveStatsReader(),
                    planStatsReader(),
                    clusterStatsReader(),
                    clusterStatsByDayLoader(),
                    dbAccessConfig.historyDbIO(),
                    dbAccessConfig.dataSource(),
                    projectedStatsStore(),
                    paginationParamsFactory(),
                    statSnapshotCreator(),
                    statRecordBuilder(),
                    systemLoadReader(),
                    systemLoadRecordsPerChunk,
                    percentileReader(),
                    movingStatisticsReader(),
                    volumeAttachmentHistoryReader());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create statsRpcService bean", e);
        }
    }

    @Bean
    protected PercentileReader percentileReader() {
        try {
            return new PercentileReader(timeToWaitNetworkReadinessMs, grpcReadingTimeoutMs, clock(),
                    dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create percentileReader bean", e);
        }
    }

    @Bean
    protected MovingStatisticsReader movingStatisticsReader() {
        try {
            return new MovingStatisticsReader(timeToWaitNetworkReadinessMs, grpcReadingTimeoutMs,
                    clock(),
                    dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create movingStatisticReader bean", e);
        }
    }

    @Bean
    protected Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * Create a {@link StatSnapshotCreator} for history.
     *
     * @return StatSnapshotCreator
     */
    @Bean
    public StatSnapshotCreator statSnapshotCreator() {
        return createStatSnapshotCreator(liveStatsReader());
    }

    protected static DefaultStatSnapshotCreator createStatSnapshotCreator(
            LiveStatsReader liveStatsReader) {
        return new DefaultStatSnapshotCreator(new ProducerIdPopulator(liveStatsReader));
    }

    /**
     * Create a {@link StatRecordBuilder} for history.
     *
     * @return StatRecordBuilder
     */
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

    /**
     * Create a {@link StatsQueryFactory} for history.
     *
     * @return StatsQueryFactory
     */
    @Bean
    public StatsQueryFactory statsQueryFactory() {
        return new DefaultStatsQueryFactory(dbAccessConfig.historyDbIO());
    }

    /**
     * Create a {@link RetentionPeriodFetcher} for history.
     *
     * @return RetentionPeriodFetcher
     */
    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(clock(), updateRetentionIntervalSeconds,
                TimeUnit.SECONDS, numRetainedMinutes,
                SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    /**
     * Create a {@link TimeFrameCalculator} for history.
     *
     * @return TimeFrameCalculator
     */
    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(clock(), retentionPeriodFetcher());
    }

    /**
     * Create a {@link DefaultTimeRangeFactory} for history.
     *
     * @return DefaultTimeRangeFactory
     */
    @Bean
    public DefaultTimeRangeFactory defaultTimeRangeFactory() {
        return new DefaultTimeRangeFactory(dbAccessConfig.historyDbIO(),
                timeFrameCalculator(),
                latestTableTimeWindowMin, TimeUnit.MINUTES);
    }

    /**
     * Create an  {@link EntityStatsPaginationParamsFactory} for history.
     *
     * @return EntityStatsPaginationParamsFactory
     */
    @Bean
    public EntityStatsPaginationParamsFactory paginationParamsFactory() {
        return new DefaultEntityStatsPaginationParamsFactory(historyPaginationDefaultLimit,
                historyPaginationMaxLimit, historyPaginationDefaultSortCommodity);
    }

    /**
     * The {@link LiveStatsStore} keeps track of stats from the most recent live topology.
     *
     * @return LiveStatsStore
     */
    @Bean
    public LiveStatsStore liveStatsStore() {
        return new LiveStatsStore(ingestersConfig.excludedCommodities(),
                new LongDataPack());
    }

    /**
     * Create a {@link ProjectedStatsStore} for history.
     *
     * @return ProjectedStatsStore
     */
    @Bean
    public ProjectedStatsStore projectedStatsStore() {
        return new ProjectedStatsStore(ingestersConfig.excludedCommodities(),
                new LongDataPack(), new DataPack<>());
    }

    /**
     * Create a {@link SystemLoadReader} for history.
     *
     * @return SystemLoadReader
     */
    @Bean
    public SystemLoadReader systemLoadReader() {
        try {
            return new SystemLoadReader(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create systemLoadReader bean", e);
        }
    }

    /**
     * Create a {@link LiveStatsReader} for history.
     *
     * @return LiveStatsReader
     */
    @Bean
    public LiveStatsReader liveStatsReader() {
        try {
            return new LiveStatsReader(dbAccessConfig.historyDbIO(),
                    dbAccessConfig.dsl(),
                    defaultTimeRangeFactory(),
                    statsQueryFactory(),
                    computedPropertiesProcessorFactory(),
                    histUtilizationReader(),
                    entitiesReadPerChunk);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create liveStatsReader bean", e);
        }
    }

    @Bean
    protected HistUtilizationReader histUtilizationReader() {
        try {
            return new HistUtilizationReader(dbAccessConfig.dsl(), entitiesReadPerChunk,
                    liveStatsStore());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create histUtilizationReader bean", e);
        }
    }

    /**
     * Create a factory that delivers {@link ComputedPropertiesProcessor} instances.
     *
     * @return factory
     */
    @Bean
    public ComputedPropertiesProcessorFactory computedPropertiesProcessorFactory() {
        return ComputedPropertiesProcessor::new;
    }

    /**
     * Create a {@link StatsHistoryServiceController} for history.
     *
     * @return StatsHistoryServiceController
     */
    @Bean
    public StatsHistoryServiceController statsRestController() {
        return new StatsHistoryServiceController(statsRpcService());
    }

    /**
     * Create a {@link PlanStatsReader} for history.
     *
     * @return PlanStatsReader
     */
    @Bean
    public PlanStatsReader planStatsReader() {
        try {
            return new PlanStatsReader(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create dbAccessConfig bean", e);
        }
    }

    /**
     * Time range factory for use with cluster stats.
     *
     * @return factory
     */
    @Bean
    public ClusterTimeRangeFactory clusterTimeRangeFactory() {
        try {
            return new ClusterTimeRangeFactory(
                    dbAccessConfig.historyDbIO(), dbAccessConfig.dsl(), timeFrameCalculator());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create clusterTimeRangeFactory bean", e);
        }
    }

    /**
     * Cluster stats reader.
     *
     * @return reader
     */
    @Bean
    public ClusterStatsReader clusterStatsReader() {
        try {
            return new ClusterStatsReader(dbAccessConfig.dsl(), clusterTimeRangeFactory(),
                    defaultTimeRangeFactory(), computedPropertiesProcessorFactory(),
                    maxAmountOfEntitiesPerGrpcMessage);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create clusterStatsReader bean", e);
        }
    }

    @Bean
    public VolumeAttachmentHistoryReader volumeAttachmentHistoryReader() {
        try {
            return new VolumeAttachmentHistoryReader(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create volumeAttachmentHistoryReader bean",
                    e);
        }
    }
}
