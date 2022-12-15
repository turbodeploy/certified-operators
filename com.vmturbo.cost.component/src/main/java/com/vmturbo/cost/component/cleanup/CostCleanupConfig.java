package com.vmturbo.cost.component.cleanup;

import static com.vmturbo.cost.component.db.Tables.BILLED_COST_DAILY;
import static com.vmturbo.cost.component.db.Tables.BILLED_COST_HOURLY;
import static com.vmturbo.cost.component.db.Tables.BILLED_COST_MONTHLY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COMPUTE_TIER_ALLOCATION;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.ENTITY_COST_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_COVERAGE_LATEST;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_UTILIZATION_LATEST;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jooq.DSLContext;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableCleanupInfo;
import com.vmturbo.cost.component.cleanup.TableCleanupWorker.TableCleanupWorkerFactory;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.persistence.DataIngestionBouncer;
import com.vmturbo.cost.component.persistence.DataIngestionBouncer.DataIngestionConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * A spring configuration for cleanup of cost tables.
 */
@Configuration
@Import({DbAccessConfig.class,
        CloudCommitmentAnalysisStoreConfig.class})
public class CostCleanupConfig {

    @Value("${retention.updateRetentionIntervalSeconds:10}")
    private int updateRetentionIntervalSeconds;

    @Value("${retention.numRetainedMinutes:130}")
    private int numRetainedMinutes;

    @Value("${tableCleanup.corePoolSize:0}")
    private int cleanupCorePoolSize;

    @Value("${tableCleanup.maxPoolSize:0}")
    private int cleanupMaxPoolSize;

    @Value("${tableCleanup.threadKeepAliveMins:1}")
    private int cleanupThreadKeepAliveMins;

    @Value("${tableCleanup.executorQueueSize:10}")
    private int cleanupExecutorQueueSize;

    @Value("${tableCleanup.taskSchedulerPoolSize:0}")
    private int taskSchedulerPoolSize;

    @Value("${tableCleanup.cleanupIntervalSeconds:3600}")
    private int cleanupIntervalSeconds;

    @Value("${tableCleanup.entityCostBatchDelete:1000}")
    private int entityCostBatchDelete;

    @Value("${tableCleanup.entityCostLatest.deleteInterval:PT1H}")
    private String entityCostLatestDeleteInterval;

    @Value("${tableCleanup.entityCostLatest.longRunningDuration:PT10M}")
    private String entityCostLatestLongRunningDuration;

    @Value("${tableCleanup.entityCostLatest.blockIngestionOnLongDelete:true}")
    private boolean entityCostBlockIngestionOnLongDelete;

    @Value("${tableCleanup.riCoverageBatchDelete:1000}")
    private int riCoverageBatchDelete;

    @Value("${tableCleanup.riCoverageLatest.deleteInterval:PT1H}")
    private String riCoverageLatestDeleteInterval;

    @Value("${tableCleanup.riUtilizationBatchDelete:1000}")
    private int riUtilizationBatchDelete;

    @Value("${tableCleanup.riUtilizationLatest.deleteInterval:PT1H}")
    private String riUtilizationLatestDeleteInterval;

    @Value("${tableCleanup.computeTierAllocationBatchDelete:1000}")
    private int computeTierAllocationBatchDelete;

    @Value("${tableCleanup.computeTierAllocation.deleteInterval:PT1H}")
    private String computeTierAllocationDeleteInterval;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private CloudCommitmentAnalysisStoreConfig ccaStoreConfig;

    @Autowired
    private SettingServiceBlockingStub settingServiceBlockingStub;

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
     * @param tableCleanups The table cleanup configurations.
     * @return The cleanup executor service.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService cleanupExecutorService(@Nonnull List<CostTableCleanup> tableCleanups) {


        final int corePoolSize = cleanupCorePoolSize > 0
                ? cleanupCorePoolSize
                : Math.max(tableCleanups.size(), 1);
        final int maxPoolSize = cleanupMaxPoolSize > 0
                ? cleanupMaxPoolSize
                : corePoolSize;

        return new ThreadPoolExecutor(corePoolSize,
                maxPoolSize,
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
     * The {@link CostTableCleanupManager}.
     * @param cleanupWorkerFactory The cleanup worker factory.
     * @param cleanupManagerScheduler The cleanup manager scheduler.
     * @param tableCleanups The table cleanups.
     * @return The {@link CostTableCleanupManager}.
     */
    @Lazy(false)
    @Bean
    public CostTableCleanupManager cleanupManager(@Nonnull TableCleanupWorkerFactory cleanupWorkerFactory,
                                                  @Nonnull @Qualifier("cleanupManagerScheduler") TaskScheduler cleanupManagerScheduler,
                                                  @Nonnull List<CostTableCleanup> tableCleanups) {

        return new CostTableCleanupManager(
                cleanupWorkerFactory,
                cleanupManagerScheduler,
                tableCleanups);
    }

    /**
     * The {@link DataIngestionBouncer}.
     * @param tableCleanups The configured table cleanups.
     * @return The {@link DataIngestionBouncer}.
     */
    @Bean
    public DataIngestionBouncer ingestionBouncer(@Nonnull List<CostTableCleanup> tableCleanups,
                                                 @Nonnull CostTableCleanupManager cleanupManager) {
        return new DataIngestionBouncer(cleanupManager,
                DataIngestionConfig.builder()
                        .addAllCleanupInfoList(tableCleanups.stream()
                                .map(CostTableCleanup::tableInfo)
                                .collect(ImmutableList.toImmutableList()))
                        .build());
    }

    /**
     * The {@link TableCleanupWorkerFactory}.
     * @param cleanupExecutorService The cleanup trimmer task executor.
     * @return The {@link TableCleanupWorkerFactory}.
     */
    @Bean
    public TableCleanupWorkerFactory tableCleanupWorkerFactory(
            @Nonnull @Qualifier("cleanupExecutorService") ExecutorService cleanupExecutorService) {

        return new TableCleanupWorkerFactory(cleanupExecutorService);
    }

    /**
     * The {@link RetentionPeriodFetcher}.
     * @return The {@link RetentionPeriodFetcher}.
     */
    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(costClock(),
                updateRetentionIntervalSeconds, TimeUnit.SECONDS,
                numRetainedMinutes, settingServiceBlockingStub);
    }

    /**
     * Time frame calculator bean.
     *
     * @return Time frame calculator bean.
     */
    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(costClock(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_MONTH}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_MONTH}.
     */
    @Bean
    public CostStatMonthlyTable coverageMonthlyStatTable() {
        final DSLContext dsl = getDslContextForBean("coverageMonthlyStatTable");
        return new CostStatMonthlyTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_COVERAGE_BY_MONTH.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_COVERAGE_BY_MONTH)
                .numRowsToBatchDelete(riCoverageBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_HOUR}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_HOUR}.
     */
    @Bean
    public CostStatHourTable coverageHourStatTable() {
        final DSLContext dsl = getDslContextForBean("coverageHourStatTable");
        return new CostStatHourTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_COVERAGE_BY_HOUR.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_COVERAGE_BY_HOUR)
                .numRowsToBatchDelete(riCoverageBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_DAY}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_BY_DAY}.
     */
    @Bean
    public CostStatDayTable coverageDayStatTable() {
        final DSLContext dsl = getDslContextForBean("coverageDayStatTable");
        return new CostStatDayTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_COVERAGE_BY_DAY.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_COVERAGE_BY_DAY)
                .numRowsToBatchDelete(riCoverageBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_LATEST}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_COVERAGE_LATEST}.
     */
    @Bean
    public CostStatLatestTable coverageLatestStatTable() {
        final DSLContext dsl = getDslContextForBean("coverageLatestStatTable");
        return new CostStatLatestTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_COVERAGE_LATEST.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_COVERAGE_LATEST)
                .numRowsToBatchDelete(riCoverageBatchDelete)
                .cleanupRate(Duration.parse(riCoverageLatestDeleteInterval))
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_MONTH}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_MONTH}.
     */
    @Bean
    public CostStatMonthlyTable utilizationMonthlyStatTable() {
        final DSLContext dsl = getDslContextForBean("utilizationMonthlyStatTable");
        return new CostStatMonthlyTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_UTILIZATION_BY_MONTH.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_UTILIZATION_BY_MONTH)
                .numRowsToBatchDelete(riUtilizationBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_HOUR}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_HOUR}.
     */
    @Bean
    public CostStatHourTable utilizationHourStatTable() {
        final DSLContext dsl = getDslContextForBean("utilizationHourStatTable");
        return new CostStatHourTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_UTILIZATION_BY_HOUR.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_UTILIZATION_BY_HOUR)
                .numRowsToBatchDelete(riUtilizationBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_DAY}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_BY_DAY}.
     */
    @Bean
    public CostStatDayTable utilizationDayStatTable() {
        final DSLContext dsl = getDslContextForBean("utilizationDayStatTable");
        return new CostStatDayTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_UTILIZATION_BY_DAY.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_UTILIZATION_BY_DAY)
                .numRowsToBatchDelete(riUtilizationBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_LATEST}.
     * @return The stats cleanup for {@link Tables#RESERVED_INSTANCE_UTILIZATION_LATEST}.
     */
    @Bean
    public CostStatLatestTable utilizationLatestTable() {
        final DSLContext dsl = getDslContextForBean("utilizationLatestTable");
        return new CostStatLatestTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(RESERVED_INSTANCE_UTILIZATION_LATEST.SNAPSHOT_TIME)
                .table(RESERVED_INSTANCE_UTILIZATION_LATEST)
                .numRowsToBatchDelete(riUtilizationBatchDelete)
                .cleanupRate(Duration.parse(riUtilizationLatestDeleteInterval))
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST}.
     */
    @Bean
    public CostStatLatestTable entityCostTable() {
        final DSLContext dsl = getDslContextForBean("entityCostTable");
        return new CostStatLatestTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(ENTITY_COST.CREATED_TIME)
                .table(ENTITY_COST)
                .numRowsToBatchDelete(entityCostBatchDelete)
                .cleanupRate(Duration.parse(entityCostLatestDeleteInterval))
                .blockIngestionOnLongRunning(entityCostBlockIngestionOnLongDelete)
                .longRunningDuration(Duration.parse(entityCostLatestLongRunningDuration))
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST_BY_HOUR}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST_BY_HOUR}.
     */
    @Bean
    public CostStatHourTable entityCostHourTable() {
        final DSLContext dsl = getDslContextForBean("entityCostHourTable");
        return new CostStatHourTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(ENTITY_COST_BY_HOUR.CREATED_TIME)
                .table(ENTITY_COST_BY_HOUR)
                .numRowsToBatchDelete(entityCostBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST_BY_DAY}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST_BY_DAY}.
     */
    @Bean
    public CostStatDayTable entityCostDayTable() {
        final DSLContext dsl = getDslContextForBean("entityCostDayTable");
        return new CostStatDayTable(dsl, costClock(),
                TableCleanupInfo.builder().timeField(ENTITY_COST_BY_DAY.CREATED_TIME).table(
                        ENTITY_COST_BY_DAY).numRowsToBatchDelete(entityCostBatchDelete).build(),
                retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#ENTITY_COST_BY_MONTH}.
     * @return The stats cleanup for {@link Tables#ENTITY_COST_BY_MONTH}.
     */
    @Bean
    public CostStatMonthlyTable entityCostMonthlyTable() {
        final DSLContext dsl = getDslContextForBean("entityCostMonthlyTable");
        return new CostStatMonthlyTable(dsl, costClock(), TableCleanupInfo.builder()
                .timeField(ENTITY_COST_BY_MONTH.CREATED_TIME)
                .table(ENTITY_COST_BY_MONTH)
                .numRowsToBatchDelete(entityCostBatchDelete)
                .build(), retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#BILLED_COST_HOURLY}.
     *
     * @return The stats cleanup for {@link Tables#BILLED_COST_HOURLY}.
     */
    @Bean
    public CostStatHourTable billedCostHourlyTable() {
        final DSLContext dsl = getDslContextForBean("billedCostHourlyTable");
        final TableCleanupInfo cleanupInfo = TableCleanupInfo.builder()
                .timeField(BILLED_COST_HOURLY.SAMPLE_TIME)
                .table(BILLED_COST_HOURLY)
                .numRowsToBatchDelete(entityCostBatchDelete)
                .build();
        return new CostStatHourTable(dsl, costClock(), cleanupInfo, retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#BILLED_COST_DAILY}.
     *
     * @return The stats cleanup for {@link Tables#BILLED_COST_DAILY}.
     */
    @Bean
    public CostStatDayTable billedCostDailyTable() {
        final DSLContext dsl = getDslContextForBean("billedCostDailyTable");
        final TableCleanupInfo cleanupInfo = TableCleanupInfo.builder()
                .timeField(BILLED_COST_DAILY.SAMPLE_TIME)
                .table(BILLED_COST_DAILY)
                .numRowsToBatchDelete(entityCostBatchDelete)
                .build();
        return new CostStatDayTable(dsl, costClock(), cleanupInfo, retentionPeriodFetcher());
    }

    /**
     * The stats cleanup for {@link Tables#BILLED_COST_MONTHLY}.
     *
     * @return The stats cleanup for {@link Tables#BILLED_COST_MONTHLY}.
     */
    @Bean
    public CostStatMonthlyTable billedCostMonthlyTable() {
        final DSLContext dsl = getDslContextForBean("billedCostMonthlyTable");
        final TableCleanupInfo cleanupInfo = TableCleanupInfo.builder()
                .timeField(BILLED_COST_MONTHLY.SAMPLE_TIME)
                .table(BILLED_COST_MONTHLY)
                .numRowsToBatchDelete(entityCostBatchDelete)
                .build();
        return new CostStatMonthlyTable(dsl, costClock(), cleanupInfo, retentionPeriodFetcher());
    }

    /**
     * The cost cleanup task for {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION}.
     * @return The cost cleanup task for {@link Tables#ENTITY_COMPUTE_TIER_ALLOCATION}.
     */
    @Bean
    public CostTableCleanup computeTierAllocationCleanup() {
        final DSLContext dsl = getDslContextForBean("computeTierAllocationCleanup");
        return new CustomRetentionCleanup(dsl, costClock(), TableCleanupInfo.builder()
                .table(ENTITY_COMPUTE_TIER_ALLOCATION)
                .timeField(ENTITY_COMPUTE_TIER_ALLOCATION.END_TIME)
                .numRowsToBatchDelete(computeTierAllocationBatchDelete)
                .cleanupRate(Duration.parse(computeTierAllocationDeleteInterval))
                .build(),
                settingsRetentionFetcher());
    }

    /**
     * Creates a new {@link SettingsRetentionFetcher} bean.
     * @return The {@link SettingsRetentionFetcher} instance.
     */
    @Bean
    public SettingsRetentionFetcher settingsRetentionFetcher() {
        return new SettingsRetentionFetcher(
                settingServiceBlockingStub,
                GlobalSettingSpecs.CloudCommitmentAllocationRetentionDays.createSettingSpec(),
                ChronoUnit.DAYS,
                Duration.ofSeconds(updateRetentionIntervalSeconds));
    }


    /**
     * Create a {@link TaskScheduler} to use in periodically
     * cleaning up cost stats table.
     *
     * @param tableCleanups The table cleanup configurations.
     * @return a {@link TaskScheduler} to use for cluster stats rollups
     */
    @Bean(destroyMethod = "shutdown")
    protected ThreadPoolTaskScheduler cleanupManagerScheduler(@Nonnull List<CostTableCleanup> tableCleanups) {

        final int poolSize = taskSchedulerPoolSize > 0
                ? taskSchedulerPoolSize
                : Math.max(tableCleanups.size(), 1);

        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.initialize();
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("CostStats-cleanup-%d").build();
    }

    private DSLContext getDslContextForBean(@Nonnull final String beanName) {
        try {
            return dbAccessConfig.dsl();
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create bean: " + beanName, e);
        }
    }
}
