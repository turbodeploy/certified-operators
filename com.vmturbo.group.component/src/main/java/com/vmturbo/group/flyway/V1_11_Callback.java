package com.vmturbo.group.flyway;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.BaseFlywayCallback;

/**
 * This callback adjusts, if necessary, the Flyway-recorded checksum for the V1.11 migration.
 *
 * <p>The migration as originally created contained a comment that started with "--" without a
 * trailing space. This is supported by the version of Flyway we are currently using, but will
 * not be supported by later versions, since it is not allowed by MySQL.</p>
 *
 * <p>See  details <a href="https://dev.mysql.com/doc/refman/8.0/en/ansi-diff-comments.html">
 * here</a>.</p>
 *
 * <p>We have replaced the migration file so that it now includes the required space, but we have
 * deployed the faulty migration file to some customers, and so its checksum is now recorded in
 * the flyway versions table, and this will cause future upgrades to fail, since the checksum of
 * the corrected file will not match the recorded checksum and pre-migration validation will fail.
 * </p>
 *
 * <p>This class implements a pre-validation callback method that checks corrects the V1.11 checksum
 * if it is incorrect, thereby allowing validation - and migrations - to succeed.</p>
 */
@SuppressWarnings("checkstyle:TypeName")
public class V1_11_Callback extends BaseFlywayCallback {
    private static final Logger logger = LogManager.getLogger();

    private static final String GROUP_COMPONENT_SCHEMA = "group_component";
    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String V1_11_VERSION = "1.11";
    static final int V1_11_CORRECT_CHECKSUM = 492223504;


    static final String TABLE_EXISTS_QUERY = String.format(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'",
        GROUP_COMPONENT_SCHEMA, FLYWAY_TABLE_NAME);
    static final String GET_CHECKSUM_QUERY = String.format(
        "SELECT checksum FROM %s.%s WHERE version=%s",
        GROUP_COMPONENT_SCHEMA, FLYWAY_TABLE_NAME, V1_11_VERSION);
    static final String UPDATE_CHECKSUM_STMT = String.format(
        "UPDATE %s.%s SET checksum = %d WHERE version='%s'",
        GROUP_COMPONENT_SCHEMA, FLYWAY_TABLE_NAME, V1_11_CORRECT_CHECKSUM, V1_11_VERSION);

    @Override
    public void beforeValidate(final Connection connection) {
        try {
            if (needsFix(connection)) {
                logger.info("Applying fix for V1.11 migration");
                applyFix(connection);
            }
        } catch (SQLException e) {
            logger.error("Failed to apply fix for V1.11 migration", e);
        }
    }

    /**
     * Check if the fix is needed.
     *
     * <p>We only need it if a V1.11 migration has been applied and if it's been recorded with the
     * correct checksum.</p>
     *
     * @param connection DB connection
     * @return true if the fix is needed
     * @throws SQLException if there's a problem any of our db operations
     */
    private boolean needsFix(Connection connection) throws SQLException {
        // check to make sure the flyway version table exists
        return schemaTableExists(connection)
            && checksumIsIncorrect(connection);
    }

    private boolean schemaTableExists(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(TABLE_EXISTS_QUERY)) {
            // if we got a row, the schema table exists
            return result.next();
        }
    }

    private boolean checksumIsIncorrect(final Connection connection) throws SQLException {
        try (ResultSet result = connection.createStatement().executeQuery(GET_CHECKSUM_QUERY)) {
            return result.next() && result.getInt(1) != V1_11_CORRECT_CHECKSUM;
        }
    }

    private void applyFix(Connection connection) throws SQLException {
        connection.createStatement().executeUpdate(UPDATE_CHECKSUM_STMT);
    }
}
