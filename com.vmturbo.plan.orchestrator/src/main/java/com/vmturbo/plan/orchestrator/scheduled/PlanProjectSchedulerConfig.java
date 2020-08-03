package com.vmturbo.plan.orchestrator.scheduled;

import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;

/**
 * Configuration for the PlanProjectScheduler package in plan.
 */
@Configuration
@Import({PlanProjectConfig.class})
public class PlanProjectSchedulerConfig {

    @Autowired
    private PlanProjectConfig planProjectConfig;

    @Value("${defaultHeadroomPlanProjectJsonFile:systemPlanProjects.json}")
    private String defaultHeadroomPlanProjectJsonFile;

    /**
     * Create a {@link TaskScheduler} to use for scheduled plan executions.
     *
     * @return a {@link TaskScheduler} to use for scheduled executions
     */
    @Bean(name = "taskScheduler")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(5);
        scheduler.setThreadFactory(threadFactory());
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    @Bean
    public PlanProjectScheduler scheduler() {
        return new PlanProjectScheduler(planProjectConfig.planProjectDao(),
                taskScheduler(),
                planProjectConfig.planProjectExecutor());
    }

    @Bean
    public SystemPlanProjectLoader systemPlanProjectLoader() throws InterruptedException {
        return new SystemPlanProjectLoader(planProjectConfig.planProjectDao(),
                scheduler(), defaultHeadroomPlanProjectJsonFile);
    }

    private ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("plan-project-%d").build();
    }

}
