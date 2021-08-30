package com.vmturbo.api.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.BaseFlywayCallback;

/**
 * This callback adjusts, if necessary, the Flyway recorded checksum for the V1_1 migration.
 *
 * <p>Make the V1_1 script to be idempotent, in case it's executed earlier but flyway schema_version
 * is not updated (could be due to the process is killed by k8s).
 * </p>
 */
public class MigrationCallbackForVersion11 extends BaseFlywayCallback {

    static final int V1_1_CORRECT_CHECKSUM = -1690930427;
    static final String GET_DATABASE_QUERY = "SELECT DATABASE()";
    private static final Logger logger = LogManager.getLogger();
    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String V1_1_VERSION = "1.1";
    static final String GET_CHECKSUM_QUERY = String.format(
            "SELECT checksum FROM %s WHERE version=%s", FLYWAY_TABLE_NAME, V1_1_VERSION);
    static final String UPDATE_CHECKSUM_STMT = String.format(
            "UPDATE %s SET checksum = %d WHERE version='%s'", FLYWAY_TABLE_NAME,
            V1_1_CORRECT_CHECKSUM, V1_1_VERSION);
    private static final String TABLE_EXISTS_QUERY =
            "SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'";
    private String dbName;

    @Override
    public void beforeValidate(final Connection connection) {
        try {
            dbName = getDatabase(connection);
            if (needsFix(connection, dbName)) {
                logger.info("Applying the V1_1 migration fix");
                applyFix(connection);
            } else {
                logger.info("Fixing is not required for the V1_1 migration.");
            }
        } catch (SQLException ex) {
            logger.error("Failed to apply the fix for the V1_1 migration", ex);
        }
    }

    /**
     * Check if the checksum fix is required.
     *
     * <p>We only need it if a V1_1 migration has been applied and if it's been recorded with the
     * incorrect checksum.</p>
     *
     * @param connection The DB connection.
     * @param dbName The database that we are working on.
     * @return true if the fix is needed.
     * @throws SQLException if the DB queries cannot be executed.
     */
    private boolean needsFix(Connection connection, String dbName) throws SQLException {
        // Check to make sure the Flyway version table exists
        return schemaTableExists(connection, dbName) && checksumIsIncorrect(connection);
    }

    private boolean schemaTableExists(final Connection connection, String dbName)
            throws SQLException {
        final String query = String.format(TABLE_EXISTS_QUERY, dbName, FLYWAY_TABLE_NAME);
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(query)) {
            // If we got a row, the schema_version table exists
            return result.next();
        }
    }

    private boolean checksumIsIncorrect(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(GET_CHECKSUM_QUERY)) {
            return (result.next() && (result.getInt(1) != V1_1_CORRECT_CHECKSUM));
        }
    }

    private String getDatabase(final Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(GET_DATABASE_QUERY)) {
            result.next();
            return result.getString(1);
        }
    }

    private void applyFix(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(UPDATE_CHECKSUM_STMT);
        }
    }
}
