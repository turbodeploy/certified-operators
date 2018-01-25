package com.vmturbo.plan.orchestrator.scenario;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario;
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;

/**
 * This class provides access to the scenario db.
 */
public class ScenarioDao {

    private final Logger logger = LogManager.getLogger();
    private final DSLContext dsl;

    public ScenarioDao(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * Create a new scenario record in the DB.
     *
     * @param scenario The new scenario which has to be pesisted.
     *
     */
    public void createScenario(Scenario scenario) {
        dsl.newRecord(SCENARIO, scenario).store();
    }

    /**
     * Get the scenario.
     *
     * @param scenarioId ID of the scenario to fetch.
     * @return Return the Scenario DTO object.
     */
    public Optional<PlanDTO.Scenario> getScenario(final long scenarioId) {
        Optional<ScenarioRecord> loadedScenario = Optional.ofNullable(
                dsl.selectFrom(SCENARIO)
                        .where(SCENARIO.ID.eq(scenarioId))
                        .fetchAny());
        return loadedScenario
                .map(record -> toScenarioDTO(record.into(Scenario.class)));
    }

    /**
     * Get all the scenarios.
     *
     * @return List of the Scenarios.
     */
    public List<Scenario> getScenarios() {
        return dsl
            .selectFrom(SCENARIO)
            .fetch()
            .into(Scenario.class);
    }

    /**
     * Update all scenarios.
     *
     * @param newScenarioInfo The new scenario info.
     * @param scenarioId Id of the scenario to update.
     * @return The number of updated rows.
     */
    public int updateScenario(ScenarioInfo newScenarioInfo, final long scenarioId) {
        return dsl.update(SCENARIO)
                    .set(SCENARIO.SCENARIO_INFO, newScenarioInfo)
                    .where(SCENARIO.ID.eq(scenarioId))
                    .execute();
    }

    /**
     * Delete a scenario.
     *
     * @param scenarioId Id of the scenario to delete.
     */
    public void deleteScenario(final long scenarioId) {
        // TODO: implement referrential integrity between PlanInstances and Scenarios
        dsl.transaction(configuration -> {
                DSL.using(configuration)
                        .delete(SCENARIO)
                        .where(SCENARIO.ID.equal(scenarioId))
                        .execute();
        });
    }

    /**
     * Convert Scenario pojo to Scenario DTO.
     *
     * @param scenario Scenario pojo object.
     * @return return converted Plan.Scenario DTO.
     */
    public static PlanDTO.Scenario toScenarioDTO(@Nonnull final Scenario scenario) {
        return PlanDTO.Scenario.newBuilder()
            .setId(scenario.getId())
            .setScenarioInfo(scenario.getScenarioInfo())
            .build();
    }
}
