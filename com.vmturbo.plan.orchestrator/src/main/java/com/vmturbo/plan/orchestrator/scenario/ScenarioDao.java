package com.vmturbo.plan.orchestrator.scenario;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario;
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;

/**
 * This class provides access to the scenario db.
 */
public class ScenarioDao implements DiagsRestorable<Void> {

    @VisibleForTesting
    static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

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
    public Optional<ScenarioOuterClass.Scenario> getScenario(final long scenarioId) {
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
    public static ScenarioOuterClass.Scenario toScenarioDTO(@Nonnull final Scenario scenario) {
        return ScenarioOuterClass.Scenario.newBuilder()
            .setId(scenario.getId())
            .setScenarioInfo(scenario.getScenarioInfo())
            .build();
    }

    /**
     * {@inheritDoc}
     *
     * This method retrieves all scenarios and serializes them as JSON strings.
     *
     * @throws DiagnosticsException on exception during diagostics
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final List<Scenario> scenarios = getScenarios();

        logger.info("Collecting diagnostics for {} scenarios", scenarios.size());
        for (Scenario scenario: scenarios) {
            appender.appendString(GSON.toJson(scenario, Scenario.class));
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method clears all existing scenarios, then deserializes and adds a list of serialized
     * scenarios from diagnostics.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link StringDiagnosable#collectDiags(DiagnosticsAppender)} ()}. Must be in the same order.
     * @throws DiagnosticsException if the db already contains scenarios, or in response
     *                              to any errors that may occur deserializing or restoring a
     *                              scenario.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        final List<String> errors = new ArrayList<>();

        final List<Scenario> preexisting = getScenarios();
        if (!preexisting.isEmpty()) {
            final int numPreexisting = preexisting.size();
            final String clearingMessage = "Clearing " + numPreexisting + " preexisting scenarios: " +
                preexisting.stream().map(scenario -> scenario.getScenarioInfo().getName())
                    .collect(Collectors.toList());
            errors.add(clearingMessage);
            logger.warn(clearingMessage);

            final int deleted = deleteAllScenarios();
            if (deleted != numPreexisting) {
                final String deletedMessage = "Failed to delete " + (numPreexisting - deleted) +
                    " preexisting scenarios: " + getScenarios().stream()
                        .map(scenario -> scenario.getScenarioInfo().getName())
                        .collect(Collectors.toList());
                logger.error(deletedMessage);
                errors.add(deletedMessage);
            }
        }


        logger.info("Adding {} serialized scenarios from diagnostics", collectedDiags.size());

        final long count = collectedDiags.stream().map(serialized -> {
                try {
                    return GSON.fromJson(serialized, Scenario.class);
                } catch (JsonParseException e) {
                    errors.add("Failed to deserialize scenario " + serialized +
                        " because of parse exception " + e.getMessage());
                    return null;
                }
            })
            .filter(Objects::nonNull).map(this::restoreScenario).filter(optional -> {
                optional.ifPresent(errors::add);
                return !optional.isPresent();
            }).count();

        logger.info("Added {} scenarios from diagnostics", count);

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "Scenarios";
    }

    /**
     * Add a scenario to the database. Note that this is used for restoring scenarios from
     * diagnostics and should NOT be used for normal operations.
     *
     * @param scenario the scenario to add.
     * @return an optional of a string representing any error that may have occurred
     */
    private Optional<String> restoreScenario(@Nonnull final Scenario scenario) {
        try {
            final int r = dsl.newRecord(SCENARIO, scenario).store();
            return r == 1 ? Optional.empty() : Optional.of("Failed to restore scenario " + scenario);
        } catch (DataAccessException e) {
            return Optional.of("Could not restore scenario " + scenario +
                " because of DataAccessException "+ e.getMessage());
        }
    }

    /**
     * Delete all scenarios. Note that this method is used only for restoring scenarios from diags
     * and should NOT be used during normal operations.
     *
     * @return the number of records deleted
     */
    private int deleteAllScenarios() {
        try {
            return dsl.deleteFrom(SCENARIO).execute();
        } catch (DataAccessException e) {
            return 0;
        }
    }
}
