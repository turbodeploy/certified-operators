package com.vmturbo.plan.orchestrator.migration;

import java.util.Objects;
import java.util.SortedMap;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedMap;

import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;

/**
 * A library for managing migration of old plans.
 */
public class PlanOrchestratorMigrationsLibrary {

    /**
     * Used to manipulate plans.
     *
     * <p>Will also make remote calls to other components to delete plan data.</p>
     */
    private final PlanDao planDao;

    /**
     * Used to manipulate scenarios.
     */
    private final ScenarioDao scenarioDao;

    /**
     * Manages all the migrations in the Plan Orchestrator.
     *
     * @param planDao used to manipulate plans
     * @param scenarioDao used to manipulate scenarios
     */
    public PlanOrchestratorMigrationsLibrary(@Nonnull final PlanDao planDao,
                                             @Nonnull final ScenarioDao scenarioDao) {
        this.planDao = Objects.requireNonNull(planDao);
        this.scenarioDao = Objects.requireNonNull(scenarioDao);
    }

    /**
     * Get the list of migrations, in the order they should be run.
     *
     * @return Sorted map of migrations by name.
     */
    public SortedMap<String, Migration> getMigrations() {
        return ImmutableSortedMap.<String, Migration>naturalOrder()
            .put(V_01_00_00__PURGE_ALL_PLANS.class.getSimpleName(),
                new V_01_00_00__PURGE_ALL_PLANS(planDao, scenarioDao))
            .build();
    }
}
