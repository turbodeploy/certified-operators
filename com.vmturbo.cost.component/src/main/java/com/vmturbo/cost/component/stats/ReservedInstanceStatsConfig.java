package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.util.Arrays;
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
import com.vmturbo.group.api.GroupClientConfig;

@Configuration
@Import({CostDBConfig.class,
        GroupClientConfig.class})
public class ReservedInstanceStatsConfig {

    @Value("${retention.updateRetentionIntervalSeconds}")
    private int updateRetentionIntervalSeconds;

    @Value("${retention.numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${reservedInstanceStatCleanup.minTimeBetweenCleanupsMinutes:60}")
    private int minTimeBetweenCleanupsMinutes;

    @Value("${reservedInstanceStatCleanup.corePoolSize:1}")
    private int cleanupCorePoolSize;

    @Value("${reservedInstanceStatCleanup.maxPoolSize:8}")
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
    public Clock reservedInstanceClock() {
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
                        .setNameFormat("RITable-cleanup-thread-%d")
                        .setDaemon(true)
                        .build(),
                new CallerRunsPolicy());
    }

    @Bean
    public ReservedInstanceStatCleanupScheduler cleanupScheduler() {
        return new ReservedInstanceStatCleanupScheduler(reservedInstanceClock(), Arrays.asList(coverageDayStatTable(), coverageLatestStatTable(), coverageHourStatTable(), coverageMonthlyStatTable(),
                utilizationDayStatTable(), utilizationHourStatTable(), utilizationLatestTable(), utilizationMonthlyStatTable()),
                retentionPeriodFetcher(), cleanupExecutorService(), minTimeBetweenCleanupsMinutes,
                TimeUnit.MINUTES, taskScheduler(), cleanupSchedulerPeriod);
    }

    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(reservedInstanceClock(),
                updateRetentionIntervalSeconds, TimeUnit.SECONDS,
                numRetainedMinutes, SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    @Bean
    public ReservedInstanceCoverageMonthlyStatTable coverageMonthlyStatTable() {
        return new ReservedInstanceCoverageMonthlyStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceCoverageMonthlyStatTable.COVERAGE_MONTHLY_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceCoverageHourStatTable coverageHourStatTable() {
        return new ReservedInstanceCoverageHourStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceCoverageHourStatTable.COVERAGE_HOUR_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceCoverageDayStatTable coverageDayStatTable() {
        return new ReservedInstanceCoverageDayStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceCoverageDayStatTable.COVERAGE_DAY_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceCoverageLatestStatTable coverageLatestStatTable() {
        return new ReservedInstanceCoverageLatestStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceCoverageLatestStatTable.COVERAGE_LATEST_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceUtilizationMonthlyStatTable utilizationMonthlyStatTable() {
        return new ReservedInstanceUtilizationMonthlyStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceUtilizationMonthlyStatTable.UTILIZATION_MONTHLY_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceUtilizationHourStatTable utilizationHourStatTable() {
        return new ReservedInstanceUtilizationHourStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceUtilizationHourStatTable.UTILIZATION_HOUR_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceUtilizationDayStatTable utilizationDayStatTable() {
        return new ReservedInstanceUtilizationDayStatTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceUtilizationDayStatTable.UTILIZATION_DAY_TABLE_INFO);
    }

    @Bean
    public ReservedInstanceUtilizationLatestTable utilizationLatestTable() {
        return new ReservedInstanceUtilizationLatestTable(sqlDatabaseConfig.dsl(), reservedInstanceClock(),
                ReservedInstanceUtilizationLatestTable.UTILIZATION_LATEST_TABLE_INFO);
    }

    /**
     * Create a {@link TaskScheduler} to use in periodically
     * cleaning up ReservedInstance stats.
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
        return new ThreadFactoryBuilder().setNameFormat("ReservedInstance-cleanup-%d").build();
    }

}
