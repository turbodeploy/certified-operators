package com.vmturbo.sql.utils.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.BaseFlywayCallback;

/**
 * This is a base class for Flyway callbacks designed to perform pre-validation operations,
 * generally some sort of manipulation of the migration history table in order to accommodate
 * migration changes that would otherwise cause validations to fail.
 */
public abstract class PreValidationMigrationCallbackBase extends BaseFlywayCallback {

    protected static final String DEFAULT_MIGRATION_TABLE_NAME = new Flyway().getTable();
    protected final Logger logger;

    /**
     * Create a new callback instance.
     */
    public PreValidationMigrationCallbackBase() {
        // log messages should identify as from the subclass, not this base class
        this.logger = LogManager.getLogger(getClass());
    }

    /**
     * Peform the operations required by this callback.
     *
     * @param connection JDBC connection
     * @throws SQLException if there's a problem
     */
    protected abstract void performCallback(Connection connection) throws SQLException;

    /**
     * Check whether this callback should be performed, based on current state of the migration
     * history.
     *
     * <p>This method is only called if the migration history table is known to exist.</p>
     *
     * @param connection JDBC connection
     * @return true if the callback should be performed
     * @throws SQLException if there's a problem
     */
    protected abstract boolean isCallbackNeeded(Connection connection) throws SQLException;

    /**
     * Describe what this callback is intended to do, for use in logs.
     *
     * <p>Should be a phrase that reasonably completes a sentence like "Attempting to ..."</p>
     *
     * @return callback action description
     */
    protected abstract String describeCallback();

    /**
     * Return the name of the migrations history table.
     *
     * <p>Override if the Flyway default is not correct for this callback.</p>
     *
     * @return migration table name
     */
    protected String getMigrationTableName() {
        return DEFAULT_MIGRATION_TABLE_NAME;
    }

    @Override
    public void beforeValidate(final Connection connection) {
        try {
            if (migrationsTableExists(connection) && isCallbackNeeded(connection)) {
                logger.info("Attempting to {}", describeCallback());
                performCallback(connection);
                logger.info("Callback succeeded");
            }
        } catch (SQLException e) {
            logger.error("Failed to {}", describeCallback(), e);
        }
    }

    /**
     * Check whether the Flyway migrations table exists in this database.
     *
     * <p>If not, migrations have not been performed in this database (meaning this is a
     * new installation.</p>
     *
     * @param connection JDBC connection
     * @return true if the migrations table exists
     * @throws SQLException if we run into problems
     */
    protected boolean migrationsTableExists(final Connection connection) {
        String sql = String.format("SELECT 1 FROM %s LIMIT 1", getMigrationTableName());
        try (ResultSet result = connection.createStatement().executeQuery(sql)) {
            // if we got a result, the table exists
            return result.next();
        } catch (SQLException e) {
            // assume failure here means table does not exist
            logger.error("Failed migration table check when attempting {}", describeCallback(), e);
            return false;
        }
    }
}
