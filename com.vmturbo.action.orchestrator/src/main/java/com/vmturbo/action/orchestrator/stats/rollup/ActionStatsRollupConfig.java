package com.vmturbo.action.orchestrator.stats.rollup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;
import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.RollupDirection;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.group.api.GroupClientConfig;

@Configuration
@Import({
    ActionOrchestratorDBConfig.class,
    GroupClientConfig.class,
    ActionOrchestratorGlobalConfig.class
})
public class ActionStatsRollupConfig {

    @Autowired
    private ActionOrchestratorDBConfig sqlDatabaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Value("${actionStatRollup.corePoolSize:1}")
    private int rollupCorePoolSize;

    @Value("${actionStatRollup.maxPoolSize:4}")
    private int rollupMaxPoolSize;

    @Value("${actionStatRollup.threadKeepAliveMins:1}")
    private int rollupThreadKeepAliveMins;

    @Value("${actionStatRollup.executorQueueSize:10}")
    private int rollupExecutorQueueSize;

    @Value("${retention.numRetainedMinutes:130}")
    private int numRetainedMinutes;

    @Value("${retention.updateRetentionIntervalSeconds:10}")
    private int updateRetentionIntervalSeconds;

    @Value("${actionStatCleanup.minTimeBetweenCleanupsMinutes:60}")
    private int minTimeBetweenCleanupsMinutes;

    @Value("${actionStatRollup.corePoolSize:1}")
    private int cleanupCorePoolSize;

    @Value("${actionStatRollup.maxPoolSize:10}")
    private int cleanupMaxPoolSize;

    @Value("${actionStatRollup.threadKeepAliveMins:1}")
    private int cleanupThreadKeepAliveMins;

    @Value("${actionStatRollup.executorQueueSize:100}")
    private int cleanupExecutorQueueSize;

    @Bean
    public ActionStatRollupScheduler rollupScheduler() {
        final List<RollupDirection> rollupDependencies = new ArrayList<>();
        rollupDependencies.add(ImmutableRollupDirection.builder()
                .fromTableReader(latestTable().reader())
                .toTableWriter(hourlyTable().writer())
                .description("latest to hourly")
                .build());
        rollupDependencies.add(ImmutableRollupDirection.builder()
                .fromTableReader(hourlyTable().reader())
                .toTableWriter(dailyTable().writer())
                .description("hourly to daily")
                .build());
        rollupDependencies.add(ImmutableRollupDirection.builder()
                .fromTableReader(dailyTable().reader())
                .toTableWriter(monthlyTable().writer())
                .description("daily to monthly")
                .build());
        return new ActionStatRollupScheduler(rollupDependencies, rollupExecutorService());
    }

    @Bean
    public ActionStatCleanupScheduler cleanupScheduler() {
        return new ActionStatCleanupScheduler(globalConfig.actionOrchestratorClock(),
            Arrays.asList(latestTable(), hourlyTable(), dailyTable(), monthlyTable()),
            retentionPeriodFetcher(),
            cleanupExecutorService(),
            minTimeBetweenCleanupsMinutes, TimeUnit.MINUTES);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService cleanupExecutorService() {
        return new ThreadPoolExecutor(cleanupCorePoolSize,
            cleanupMaxPoolSize,
            cleanupThreadKeepAliveMins,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(cleanupExecutorQueueSize),
            new ThreadFactoryBuilder()
                .setNameFormat("action-cleanup-thread-%d")
                .setDaemon(true)
                .build(),
            new CallerRunsPolicy());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService rollupExecutorService() {
        return new ThreadPoolExecutor(rollupCorePoolSize,
            rollupMaxPoolSize,
            rollupThreadKeepAliveMins,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(rollupExecutorQueueSize),
            new ThreadFactoryBuilder()
                .setNameFormat("action-rollup-thread-%d")
                .setDaemon(true)
                .build(),
            new CallerRunsPolicy());
    }

    @Bean
    public LatestActionStatTable latestTable() {
        return new LatestActionStatTable(sqlDatabaseConfig.dsl(),
                globalConfig.actionOrchestratorClock(),
                rolledUpStatCalculator(), HourActionStatTable.HOUR_TABLE_INFO);
    }

    @Bean
    public HourActionStatTable hourlyTable() {
        return new HourActionStatTable(sqlDatabaseConfig.dsl(),
                globalConfig.actionOrchestratorClock(),
                rolledUpStatCalculator(), DayActionStatTable.DAY_TABLE_INFO);
    }

    @Bean
    public DayActionStatTable dailyTable() {
        return new DayActionStatTable(sqlDatabaseConfig.dsl(),
                globalConfig.actionOrchestratorClock(),
                rolledUpStatCalculator(), MonthActionStatTable.MONTH_TABLE_INFO);
    }

    @Bean
    public MonthActionStatTable monthlyTable() {
        return new MonthActionStatTable(sqlDatabaseConfig.dsl(),
                globalConfig.actionOrchestratorClock());
    }

    @Bean
    public RolledUpStatCalculator rolledUpStatCalculator() {
        return new RolledUpStatCalculator();
    }

    /**
     * This may not be the best place for this bean, since it's not strictly rollup-specific.
     * But leaving it here for now, because it's needed by the cleanup scheduler.
     */
    @Bean
    public RetentionPeriodFetcher retentionPeriodFetcher() {
        return new RetentionPeriodFetcher(globalConfig.actionOrchestratorClock(),
            updateRetentionIntervalSeconds, TimeUnit.SECONDS,
            numRetainedMinutes, SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }
}
