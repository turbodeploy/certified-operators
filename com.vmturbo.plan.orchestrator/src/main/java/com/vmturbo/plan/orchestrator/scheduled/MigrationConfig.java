package com.vmturbo.plan.orchestrator.scheduled;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.vmturbo.plan.orchestrator.migration.PlanOrchestratorMigrationsLibrary;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;

/**
 * Spring Configuration for migration of old plans.
 *
 **/
@Configuration
@EnableScheduling
@Import({PlanConfig.class,
    ScenarioConfig.class})
public class MigrationConfig {

    /**
     * Used to manipulate plans.
     */
    @Autowired
    private PlanDao planDao;

    /**
     * Used to manipulate scenarios.
     */
    @Autowired
    private ScenarioDao scenarioDao;

    /**
     * Manages all the migrations in the Plan Orchestrator.
     *
     * @return an instance of the PlanOrchestratorMigrationsLibrary
     */
    @Bean
    public PlanOrchestratorMigrationsLibrary planOrchestratorMigrationsLibrary() {
        return new PlanOrchestratorMigrationsLibrary(planDao, scenarioDao);
    }
}
