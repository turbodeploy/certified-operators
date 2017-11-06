package com.vmturbo.plan.orchestrator.scheduled;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

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

    @Bean
    public PlanProjectScheduler scheduler() {
        return new PlanProjectScheduler(planProjectConfig.planProjectDao(),
                clusterRollupSchedulerConfig.taskScheduler());
    }
}
