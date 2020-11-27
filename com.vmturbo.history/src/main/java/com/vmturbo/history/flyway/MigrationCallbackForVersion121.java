package com.vmturbo.history.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.BaseFlywayCallback;

/**
 * This callback adjusts, if necessary, the Flyway recorded checksum for the V1.21 migration.
 *
 * <p>We have replaced the legacy migration script with a new one, which is idempotent. Therefore,
 * even if the component restarts in the middle of migration the migration will not fail.
 * </p>
 *
 */
public class MigrationCallbackForVersion121 extends BaseFlywayCallback {

    private static final Logger logger = LogManager.getLogger();

    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String V1_21_VERSION = "1.21";
    static final int V1_21_CORRECT_CHECKSUM = 46932240;

    static final String GET_DATABASE_QUERY = "SELECT DATABASE()";
    private static final String TABLE_EXISTS_QUERY =
        "SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'";
    static final String GET_CHECKSUM_QUERY = String.format(
        "SELECT checksum FROM %s WHERE version=%s",
        FLYWAY_TABLE_NAME, V1_21_VERSION);
    static final String UPDATE_CHECKSUM_STMT = String.format(
        "UPDATE %s SET checksum = %d WHERE version='%s'",
        FLYWAY_TABLE_NAME, V1_21_CORRECT_CHECKSUM, V1_21_VERSION);

    private String dbName;

    @Override
    public void beforeValidate(final Connection connection) {
        try {
            dbName = getDatabase(connection);
            if (needsFix(connection, dbName)) {
                logger.info("Applying the V1.21 migration fix");
                applyFix(connection);
            } else {
                logger.info("Fixing is not required for the V1.21 migration.");
            }
        } catch (SQLException ex) {
            logger.error("Failed to apply the fix for the V1.21 migration", ex);
        }
    }

    /**
     * Check if the checksum fix is required.
     *
     * <p>We only need it if a V1.21 migration has been applied and if it's been recorded with the
     * incorrect checksum.</p>
     *
     * @param connection The DB connection.
     * @param dbName The database that we are working on.
     *
     * @return true if the fix is needed.
     * @throws SQLException if the DB queries cannot be executed.
     */
    private boolean needsFix(Connection connection, String dbName) throws SQLException {
        // Check to make sure the Flyway version table exists
        return schemaTableExists(connection, dbName)
            && checksumIsIncorrect(connection);
    }

    private boolean schemaTableExists(final Connection connection, String dbName) throws SQLException {
        String query = String.format(TABLE_EXISTS_QUERY, dbName, FLYWAY_TABLE_NAME);
        try (ResultSet result = connection.createStatement().executeQuery(query)) {
            // If we got a row, the schema_version table exists
            return result.next();
        }
    }

    private boolean checksumIsIncorrect(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(GET_CHECKSUM_QUERY)) {
            return (result.next() && (result.getInt(1) != V1_21_CORRECT_CHECKSUM));
        }
    }

    private String getDatabase(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(GET_DATABASE_QUERY)) {
            result.next();
            return result.getString(1);
        }
    }

    private void applyFix(Connection connection) throws SQLException {
        connection.createStatement().executeUpdate(UPDATE_CHECKSUM_STMT);
    }

}
