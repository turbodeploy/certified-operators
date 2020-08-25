package com.vmturbo.plan.orchestrator.scheduled;

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
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;

/**
 * Spring Configuration for deletion of old/expired plans.
 *
 **/
@Configuration
@EnableScheduling
@Import({GroupClientConfig.class,
         PlanConfig.class,
         ScenarioConfig.class,
         PlanOrchestratorDBConfig.class,
         PlanProjectConfig.class})
public class PlanDeletionSchedulerConfig implements SchedulingConfigurer {

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private PlanDao planDao;

    @Autowired
    private ScenarioDao scenarioDao;

    @Autowired
    private PlanProjectDao planProjectDao;

    /**
     * Time to run plan deletion every day.
     *
     * <p>Required format: HH:MM[:SS]</p>
     */
    @Value("${planDeletionTime:02:00}")
    private String planDeletionTime;

    /** This parameter is used to control the batch size for deleting old plans.
     *  Currently this just limits the number of old planIDs fetched from the DB.
     */
    @Value("${planDeletionBatchSize}")
    private int batchSize;

    /**
     * We will be busy looping and deleting the plans one by one. The concern is
     * that this would lead to a lot of small disk writes(as it involves DB
     * operations) and keeps the disk busy. This delay is a way to yeild the disk
     * for other tasks instead of hogging the disk for plan deletion.
     */
    @Value("${planDeletionDelayBetweenDeletesSeconds}")
    private int delayBetweenDeletesInSeconds;

    @Bean
    public PlanDeletionTask planDeletionTask() {
        return new PlanDeletionTask(settingServiceClient(), planDao, planProjectDao, scenarioDao,
                    dbConfig.dsl(), threadPoolTaskScheduler(), cronTrigger(),
                    batchSize, delayBetweenDeletesInSeconds);
    }

    /**
     * Create a trigger based on the plan deletion time defined in the configuration property.

     * @return a {@link CronTrigger} which will implement the schedule for plan deletion.
     */
    private CronTrigger cronTrigger() {
        LocalTime time = LocalTime.parse(planDeletionTime);
        final int hour = time.getHour();
        final int minute = time.getMinute();
        final int second = time.getSecond();
        String cronSpec = String.format("%d %d %d * * *", second, minute, hour);
        return new CronTrigger(cronSpec);
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setScheduler(threadPoolTaskScheduler());
    }

    /**
     * Create TaskScheduler for deleting old plans.
     * @return a {@link TaskScheduler} to use for deleting old plans.
     */
    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("scheduled-plan-deletion-thread-%d").build();
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }
}
