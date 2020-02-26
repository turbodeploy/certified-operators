package com.vmturbo.plan.orchestrator.scheduled;

import java.time.Clock;
import java.time.LocalTime;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.kvstore.KeyValueStoreConfig;

/**
 * Spring Configuration for the Plan Orchestrator scheduled tasks.
 *
 * The schedule is specified by a Configuration Property 'clusterRollupSchedule'.
 * See the declaration below for the structure of this configuration string.
 **/
@Configuration
@EnableScheduling
@Import({GroupClientConfig.class, HistoryClientConfig.class, KeyValueStoreConfig.class})
public class ClusterRollupSchedulerConfig {

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private KeyValueStoreConfig keyValueStoreConfig;

    // format is HH:MM[:SS]
    @Value("${clusterRollupTime:02:00}")
    private String clusterRollupTime;


    @Bean
    public ClusterRollupTask clusterRollupTask() {
        return new ClusterRollupTask(statsRpcService(), groupRpcService(),
                taskScheduler(), cronTrigger(), keyValueStoreConfig.keyValueStore(), Clock.systemUTC());
    }

    @Bean
    public StatsHistoryServiceBlockingStub statsRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel());
    }

    @Bean
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Create a trigger based on the rollup time defined in the configuration property.

     * @return a {@link CronTrigger} which will implement the schedule for rollups
     */
    private CronTrigger cronTrigger() {
        LocalTime time = LocalTime.parse(clusterRollupTime);
        final int hour = time.getHour();
        final int minute = time.getMinute();
        final int second = time.getSecond();
        String cronSpec = String.format("%d %d %d * * *", second, minute, hour);
        return new CronTrigger(cronSpec);
    }

    /**
     * Create a {@link TaskScheduler} to use in periodically
     * rolling up cluster stats.
     * @return a {@link TaskScheduler} to use for cluster stats rollups
     */
    @Bean(name = "taskScheduler")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("cluster-rollup-%d").build();
    }
}
