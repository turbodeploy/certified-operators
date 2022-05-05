package db.migration;

import java.sql.Connection;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import db.migrations.planorchestrator.common.V2v13RemoveResizeForVms;

/**
 * Migration to remove all the Resize settings from the scenario of the plans. We store scenarios
 * in two tables: the scenario table and the plan instance table. We first iterate the Scenario
 * table to find any Resize settings for vms and substitute them with the vmResizeSettings. We then
 * keep track of that Scenario id with the new corresponding Scenario Info in the table scenarioIdToInfo.
 * We then iterate the plan_instance table and check for every ScenarioInfo. if the ScenarioInfo is
 * contained in the table, it gets substituted with the new ScenarioInfo.
 */
public class V2v13RemoveResizeForVmsLegacy implements JdbcMigration, MigrationInfoProvider {

    /**
     * Create a new instance of the delegate migration and migrate.
     */
    @Override
    public void migrate(Connection connection) throws Exception {
        new V2v13RemoveResizeForVms().migrate(connection);
    }

    @Override
    public MigrationVersion getVersion() {
        return MigrationVersion.fromVersion("2.13");
    }

    @Override
    public String getDescription() {
        return "remove resize for vms";
    }
}
