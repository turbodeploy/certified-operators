package com.vmturbo.action.orchestrator.stats.rollup;

import java.util.ArrayList;
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

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.RollupDirection;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({
    SQLDatabaseConfig.class,
    ActionOrchestratorGlobalConfig.class
})
public class ActionStatsRollupConfig {

    @Autowired
    private SQLDatabaseConfig sqlDatabaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    @Value("${actionStatRollup.corePoolSize}")
    private int corePoolSize;

    @Value("${actionStatRollup.maxPoolSize}")
    private int maxPoolSize;

    @Value("${actionStatRollup.threadKeepAliveMins}")
    private int threadKeepAliveMins;

    @Value("${actionStatRollup.executorQueueSize}")
    private int executorQueueSize;

    @Bean
    public ActionStatRollupScheduler rollupScheduler() {
        final List<RollupDirection> rollupDependencies = new ArrayList<>();
        rollupDependencies.add(ImmutableRollupDirection.builder()
                .fromTableReader(latestTable().reader().get())
                .toTableWriter(hourlyTable().writer().get())
                .description("latest to hourly")
                .build());
        rollupDependencies.add(ImmutableRollupDirection.builder()
                .fromTableReader(hourlyTable().reader().get())
                .toTableWriter(dailyTable().writer().get())
                .description("hourly to daily")
                .build());
        rollupDependencies.add(ImmutableRollupDirection.builder()
                .fromTableReader(dailyTable().reader().get())
                .toTableWriter(monthlyTable().writer().get())
                .description("daily to monthly")
                .build());
        return new ActionStatRollupScheduler(rollupDependencies, executorService());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService executorService() {
        return new ThreadPoolExecutor(corePoolSize,
            maxPoolSize,
            threadKeepAliveMins,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(executorQueueSize),
            new ThreadFactoryBuilder()
                .setNameFormat("action-rollup-thread-%d")
                .setDaemon(true)
                .build(),
            new CallerRunsPolicy());
    }

    @Bean
    public LatestActionStatTable latestTable() {
        return new LatestActionStatTable(sqlDatabaseConfig.dsl(),
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
}
