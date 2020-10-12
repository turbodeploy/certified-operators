package com.vmturbo.plan.orchestrator.scheduled;

import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

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



    @Bean
    public PlanProjectScheduler scheduler() {
        return new PlanProjectScheduler(planProjectConfig.planProjectDao(),
                planProjectConfig.taskScheduler(),
                planProjectConfig.planProjectExecutor());
    }

    @Bean
    public SystemPlanProjectLoader systemPlanProjectLoader() throws InterruptedException {
        return new SystemPlanProjectLoader(planProjectConfig.planProjectDao(),
                scheduler(), defaultHeadroomPlanProjectJsonFile);
    }
}
