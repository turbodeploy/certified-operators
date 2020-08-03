package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;
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
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.group.api.GroupClientConfig;

@Configuration
@Import({CostDBConfig.class,
        GroupClientConfig.class})
public class CostStatsConfig {

    @Value("${retention.updateRetentionIntervalSeconds}")
    private int updateRetentionIntervalSeconds;

    @Value("${retention.numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${reservedInstanceStatCleanup.minTimeBetweenCleanupsMinutes:60}")
    private int minTimeBetweenCleanupsMinutes;

    @Value("${reservedInstanceStatCleanup.corePoolSize:1}")
    private int cleanupCorePoolSize;

    @Value("${reservedInstanceStatCleanup.maxPoolSize:12}")
    private int cleanupMaxPoolSize;

    @Value("${reservedInstanceStatCleanup.threadKeepAliveMins:1}")
    private int cleanupThreadKeepAliveMins;

    @Value("${reservedInstanceStatCleanup.executorQueueSize:10}")
    private int cleanupExecutorQueueSize;

    @Value("${reservedInstanceStatCleanup.scheduler:360000}")
    private int cleanupSchedulerPeriod;

    @Autowired
    private CostDBConfig sqlDatabaseConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    /**
     * Get the instance of the clock.
     *
     * @return The clock.
     */
    @Bean
    public Clock costClock() {
        return Clock.systemUTC();
    }

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

    @Bean
    public CostStatCleanupScheduler cleanupScheduler() {
        final List<CostStatTable> tablesToBeCleanedUp = Arrays.asList(coverageDayStatTable(), coverageLatestStatTable(),
                coverageHourStatTable(), coverageMonthlyStatTable(), utilizationDayStatTable(), utilizationHourStatTable(),
                utilizationLatestTable(), utilizationMonthlyStatTable(), entityCostTable(), entityCostDayTable(),
                entityCostHourTable(), entityCostMonthlyTable());
        return new CostStatCleanupScheduler(costClock(), tablesToBeCleanedUp,
                retentionPeriodFetcher(), cleanupExecutorService(), minTimeBetweenCleanupsMinutes,
                TimeUnit.MINUTES, taskScheduler(), cleanupSchedulerPeriod);
    }

    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(costClock(),
                updateRetentionIntervalSeconds, TimeUnit.SECONDS,
                numRetainedMinutes, SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    @Bean
    public CostStatMonthlyTable coverageMonthlyStatTable() {
        return new CostStatMonthlyTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH).shortTableName("Coverage_monthly")
                        .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build());
    }

    @Bean
    public CostStatHourTable coverageHourStatTable() {
        return new CostStatHourTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR).shortTableName("Coverage_hourly")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build());
    }

    @Bean
    public CostStatDayTable coverageDayStatTable() {
        return new CostStatDayTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY).shortTableName("Coverage_daily")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build());
    }

    @Bean
    public CostStatLatestTable coverageLatestStatTable() {
        return new CostStatLatestTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_LATEST.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_COVERAGE_LATEST).shortTableName("Coverage_latest")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES)).build());
    }

    @Bean
    public CostStatMonthlyTable utilizationMonthlyStatTable() {
        return new CostStatMonthlyTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH).shortTableName("Utilization_Monthly")
                        .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build());
    }

    @Bean
    public CostStatHourTable utilizationHourStatTable() {
        return new CostStatHourTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR).shortTableName("Utilization_hourly")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build());
    }

    @Bean
    public CostStatDayTable utilizationDayStatTable() {
        return new CostStatDayTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).shortTableName("Utilization_daily")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build());
    }

    @Bean
    public CostStatLatestTable utilizationLatestTable() {
        return new CostStatLatestTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST.SNAPSHOT_TIME)
                        .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).shortTableName("Utilization_Latest")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES)).build());
    }

    @Bean
    public CostStatLatestTable entityCostTable() {
        return new CostStatLatestTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST.CREATED_TIME)
                        .statTable(Tables.ENTITY_COST).shortTableName("entity_cost")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES)).build());
    }

    @Bean
    public CostStatHourTable entityCostHourTable() {
        return new CostStatHourTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST_BY_HOUR.CREATED_TIME)
                        .statTable(Tables.ENTITY_COST_BY_HOUR).shortTableName("entity_cost_by_hour")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build());
    }

    @Bean
    public CostStatDayTable entityCostDayTable() {
        return new CostStatDayTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST_BY_DAY.CREATED_TIME)
                        .statTable(Tables.ENTITY_COST_BY_DAY).shortTableName("entity_cost_by_day")
                        .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build());
    }

    @Bean
    public CostStatMonthlyTable entityCostMonthlyTable() {
        return new CostStatMonthlyTable(sqlDatabaseConfig.dsl(), costClock(),
                ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST_BY_MONTH.CREATED_TIME)
                        .statTable(Tables.ENTITY_COST_BY_MONTH).shortTableName("entity_cost_by_month")
                        .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build());
    }


    /**
     * Create a {@link TaskScheduler} to use in periodically
     * cleaning up cost stats table.
     * @return a {@link TaskScheduler} to use for cluster stats rollups
     */
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("CostStats-cleanup-%d").build();
    }

}
