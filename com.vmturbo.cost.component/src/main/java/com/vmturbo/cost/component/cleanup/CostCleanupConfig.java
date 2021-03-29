package com.vmturbo.cost.component.cleanup;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COMPUTE_TIER_ALLOCATION;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.HIST_ENTITY_RESERVED_INSTANCE_MAPPING;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_LATEST;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_LATEST;

import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.group.api.GroupClientConfig;

/**
 * A spring configuration for cleanup of cost tables.
 */
@Configuration
@Import({CostDBConfig.class,
        GroupClientConfig.class,
        CloudCommitmentAnalysisStoreConfig.class})
public class CostCleanupConfig {

    @Value("${retention.updateRetentionIntervalSeconds:10}")
    private int updateRetentionIntervalSeconds;

    @Value("${retention.numRetainedMinutes:130}")
    private int numRetainedMinutes;

    @Value("${histEntityRiCoverageRecordsRollingWindowDays:60}")
    private long histEntityRiCoverageRecordsRollingWindowDays;

    @Value("${tableCleanup.corePoolSize:1}")
    private int cleanupCorePoolSize;

    @Value("${tableCleanup.maxPoolSize:12}")
    private int cleanupMaxPoolSize;

    @Value("${tableCleanup.threadKeepAliveMins:1}")
    private int cleanupThreadKeepAliveMins;

    @Value("${tableCleanup.executorQueueSize:10}")
    private int cleanupExecutorQueueSize;

    @Value("${tableCleanup.cleanupIntervalSeconds:3600}")
    private int cleanupIntervalSeconds;

    @Autowired
    private CostDBConfig sqlDatabaseConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private CloudCommitmentAnalysisStoreConfig ccaStoreConfig;

    /**
     * Get the instance of the clock.
     *
     * @return The clock.
     */
    @Bean
    public Clock costClock() {
        return Clock.systemUTC();
    }

    /**
     * The cleanup executor service.
     *
     * @return The cleanup executor service.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService cleanupExecutorService() {
        return new ThreadPoolExecutor(cleanupCorePoolSize,
                cleanupMaxPoolSize,
                cleanupThreadKeepAliveMins,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(cleanupExecutorQueueSize),
                new ThreadFactoryBuilder()
                        .setNameFormat("CostTable-cleanup-thread-%d")
                        .setDaemon(true)
                        .build(),
                new CallerRunsPolicy());
    }

    /**
     * The {@link CostTableCleanupScheduler}.
     * @return The {@link CostTableCleanupScheduler}.
     */
    @Lazy(false)
    @Bean
    public CostTableCleanupScheduler cleanupScheduler() {
        final List<CostTableCleanup> tablesToBeCleanedUp = Arrays.asList(coverageDayStatTable(), coverageLatestStatTable(),
                coverageHourStatTable(), coverageMonthlyStatTable(), utilizationDayStatTable(), utilizationHourStatTable(),
                utilizationLatestTable(), utilizationMonthlyStatTable(), entityCostTable(), entityCostDayTable(),
                entityCostHourTable(), entityCostMonthlyTable(), coverageHistoricalRiPerEntityTable(),
                computeTierAllocationCleanup());

        return new CostTableCleanupScheduler(
                tablesToBeCleanedUp,
                cleanupExecutorService(),
                taskScheduler(),
                Duration.ofSeconds(cleanupIntervalSeconds));
    }

    /**
     * The {@link SettingServiceBlockingStub}.
     * @return The {@link SettingServiceBlockingStub}.
     */
    @Bean
    SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * The {@link RetentionPeriodFetcher}.
     * @return The {@link RetentionPeriodFetcher}.
     */
    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(costClock(),
                updateRetentionIntervalSeconds, TimeUnit.SECONDS,
                numRetainedMinutes, settingServiceClient());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_MONTH}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_MONTH}.
     */
    @Bean
    public CostStatMonthlyTable coverageMonthlyStatTable() {
        return new CostStatMonthlyTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_COVERAGE_BY_MONTH.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_COVERAGE_BY_MONTH).shortTableName("Coverage_monthly")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_HOUR}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_HOUR}.
     */
    @Bean
    public CostStatHourTable coverageHourStatTable() {
        return new CostStatHourTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_COVERAGE_BY_HOUR.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_COVERAGE_BY_HOUR).shortTableName("Coverage_hourly")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_DAY}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_DAY}.
     */
    @Bean
    public CostStatDayTable coverageDayStatTable() {
        return new CostStatDayTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_COVERAGE_BY_DAY.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_COVERAGE_BY_DAY)
                        .shortTableName("Coverage_daily")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_LATEST}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_LATEST}.
     */
    @Bean
    public CostStatLatestTable coverageLatestStatTable() {
        return new CostStatLatestTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_COVERAGE_LATEST.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_COVERAGE_LATEST)
                        .shortTableName("Coverage_latest")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_MONTH}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_MONTH}.
     */
    @Bean
    public CostStatMonthlyTable utilizationMonthlyStatTable() {
        return new CostStatMonthlyTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_UTILIZATION_BY_MONTH.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_UTILIZATION_BY_MONTH)
                        .shortTableName("Utilization_Monthly")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_HOUR}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_HOUR}.
     */
    @Bean
    public CostStatHourTable utilizationHourStatTable() {
        return new CostStatHourTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_UTILIZATION_BY_HOUR.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_UTILIZATION_BY_HOUR)
                        .shortTableName("Utilization_hourly")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_DAY}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_DAY}.
     */
    @Bean
    public CostStatDayTable utilizationDayStatTable() {
        return new CostStatDayTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_UTILIZATION_BY_DAY.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_UTILIZATION_BY_DAY)
                        .shortTableName("Utilization_daily")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_LATEST}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_LATEST}.
     */
    @Bean
    public CostStatLatestTable utilizationLatestTable() {
        return new CostStatLatestTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(RESERVED_INSTANCE_UTILIZATION_LATEST.SNAPSHOT_TIME)
                        .table(RESERVED_INSTANCE_UTILIZATION_LATEST)
                        .shortTableName("Utilization_Latest")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST}.
     */
    @Bean
    public CostStatLatestTable entityCostTable() {
        return new CostStatLatestTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(ENTITY_COST.CREATED_TIME)
                        .table(ENTITY_COST)
                        .shortTableName("entity_cost")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST_BY_HOUR}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST_BY_HOUR}.
     */
    @Bean
    public CostStatHourTable entityCostHourTable() {
        return new CostStatHourTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(ENTITY_COST_BY_HOUR.CREATED_TIME)
                        .table(ENTITY_COST_BY_HOUR)
                        .shortTableName("entity_cost_by_hour")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST_BY_DAY}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST_BY_DAY}.
     */
    @Bean
    public CostStatDayTable entityCostDayTable() {
        return new CostStatDayTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(ENTITY_COST_BY_DAY.CREATED_TIME)
                        .table(ENTITY_COST_BY_DAY)
                        .shortTableName("entity_cost_by_day")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST_BY_MONTH}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST_BY_MONTH}.
     */
    @Bean
    public CostStatMonthlyTable entityCostMonthlyTable() {
        return new CostStatMonthlyTable(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(ENTITY_COST_BY_MONTH.CREATED_TIME)
                        .table(ENTITY_COST_BY_MONTH)
                        .shortTableName("entity_cost_by_month")
                        .build(),
                retentionPeriodFetcher());
    }

    /**
     * The cleanup task for {@link Tables#HIST_ENTITY_RESERVED_INSTANCE_MAPPING}.
     * @return The cleanup task for {@link Tables#HIST_ENTITY_RESERVED_INSTANCE_MAPPING}.
     */
    @Bean
    public CostTableCleanup coverageHistoricalRiPerEntityTable() {
        return new CustomRetentionCleanup(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .timeField(HIST_ENTITY_RESERVED_INSTANCE_MAPPING.SNAPSHOT_TIME)
                        .table(HIST_ENTITY_RESERVED_INSTANCE_MAPPING)
                        .shortTableName("Coverage_histRI")
                        .build(),
                RetentionDurationFetcher.staticFetcher(histEntityRiCoverageRecordsRollingWindowDays, ChronoUnit.DAYS));
    }

    /**
     * The cost cleanup task for {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION}.
     * @return The cost cleanup task for {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION}.
     */
    @Bean
    public CostTableCleanup computeTierAllocationCleanup() {
        return new CustomRetentionCleanup(
                sqlDatabaseConfig.dsl(),
                costClock(),
                ImmutableTableInfo.builder()
                        .table(ENTITY_COMPUTE_TIER_ALLOCATION)
                        .timeField(ENTITY_COMPUTE_TIER_ALLOCATION.END_TIME)
                        .shortTableName("entity_compute_tier_allocation")
                        .build(),
                new SettingsRetentionFetcher(
                        settingServiceClient(),
                        GlobalSettingSpecs.CloudCommitmentAllocationRetentionDays.createSettingSpec(),
                        ChronoUnit.DAYS,
                        Duration.ofSeconds(updateRetentionIntervalSeconds)));
    }


    /**
     * Create a {@link TaskScheduler} to use in periodically
     * cleaning up cost stats table.
     * @return a {@link TaskScheduler} to use for cluster stats rollups
     */
    @Bean(destroyMethod = "shutdown")
    protected ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.initialize();
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("CostStats-cleanup-%d").build();
    }

}
