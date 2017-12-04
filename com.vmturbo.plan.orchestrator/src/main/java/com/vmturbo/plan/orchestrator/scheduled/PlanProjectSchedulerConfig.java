package com.vmturbo.plan.orchestrator.scheduled;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;

/**
 * Configuration for the PlanProjectScheduler package in plan.
 */
@Configuration
@Import({ClusterRollupSchedulerConfig.class, PlanProjectConfig.class })
public class PlanProjectSchedulerConfig {
    @Autowired
    private ClusterRollupSchedulerConfig clusterRollupSchedulerConfig;

    @Autowired
    private PlanProjectConfig planProjectConfig;

    @Value("${defaultHeadroomPlanProjectJsonFile:systemPlanProjects.json}")
    private String defaultHeadroomPlanProjectJsonFile;

    @Bean
    public PlanProjectScheduler scheduler() {
        return new PlanProjectScheduler(planProjectConfig.planProjectDao(),
                clusterRollupSchedulerConfig.taskScheduler(),
                planProjectConfig.planProjectExecutor());
    }
}
