package com.vmturbo.plan.orchestrator.scheduled;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;

/**
 * Spring Configuration for the Plan Orchestrator scheduled tasks.
 *
 * The schedule is specified by a Configuration Property 'clusterRollupSchedule'.
 * See the declaration below for the structure of this configuration string.
 **/
@Configuration
@EnableScheduling
public class ClusterRollupSchedulerConfig {

    /**
     * Perform Cluster Rollup based on a cron schedule specified in the
     * configuration property 'clusterRollupSchedule'.
     *
     * The fields of this property, are:
     *    second (0-60) ,minute (0-59), hour(0-23), day of month(1-31), month(1-12),
     *    day of week(0-7, 7=sun)
     *  a field may be an asterisk (*)
     *  For more see {@link org.springframework.scheduling.support.CronSequenceGenerator}
     *
     *  Example - every day at 1AM:
     *      0 0 1 * * *
     */
    @Value("${clusterRollupSchedule}")
    private String clusterRollupSchedule;

    @Value("${historyHost}")
    private String historyHost;

    @Value("${groupHost}")
    private String groupHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Bean
    public ClusterRollupTask clusterRollupTask() {
        return new ClusterRollupTask(statsRpcService(), groupRpcService(),
                taskScheduler(), cronTrigger());
    }

    @Bean
    public StatsHistoryServiceBlockingStub statsRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyChannel());
    }

    @Bean
    public Channel historyChannel() {
        return PingingChannelBuilder.forAddress(historyHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupChannel());
    }

    @Bean
    public Channel groupChannel() {
        return PingingChannelBuilder.forAddress(groupHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    /**
     * Create a trigger based on the rollup schedule defined in the configuration property.
     * @return a {@link CronTrigger} which will implement the schedule for rollups
     */
    @Bean
    public CronTrigger cronTrigger() {
        return new CronTrigger(clusterRollupSchedule);
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
