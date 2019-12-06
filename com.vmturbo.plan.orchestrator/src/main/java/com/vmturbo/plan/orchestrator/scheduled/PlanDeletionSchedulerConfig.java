package com.vmturbo.plan.orchestrator.scheduled;

import java.util.concurrent.ThreadFactory;

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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
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
        PlanOrchestratorDBConfig.class})
public class PlanDeletionSchedulerConfig implements SchedulingConfigurer {

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PlanOrchestratorDBConfig dbConfig;

    @Autowired
    private PlanDao planDao;

    @Autowired
    private ScenarioDao scenarioDao;

    /**
     * Plan deletion schedule. It uses the standard cron time format.
     * For more see {@link org.springframework.scheduling.support.CronSequenceGenerator}
     */
    @Value("${planDeletionSchedule}")
    private String planDeletionSchedule;

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
        return new PlanDeletionTask(settingServiceClient(), planDao, scenarioDao,
                    dbConfig.dsl(), threadPoolTaskScheduler(), cronTrigger(),
                    batchSize, delayBetweenDeletesInSeconds);
    }

    /**
     * Create a trigger based on the deletion schedule defined in the configuration property.
     * @return a {@link CronTrigger} which will implement the schedule for plan deletion.
     */
    public CronTrigger cronTrigger() {
        return new CronTrigger(planDeletionSchedule);
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
