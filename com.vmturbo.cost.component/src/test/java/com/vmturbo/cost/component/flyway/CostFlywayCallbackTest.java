package com.vmturbo.cost.component.flyway;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import com.vmturbo.cost.component.flyway.CostFlywayCallback.MigrationUpdate;
import com.vmturbo.cost.component.flyway.CostFlywayCallback.MigrationUpdates;

/**
 * Test that the Flyway callback class to fix the V1.26 migration responds correctly
 * in all circumstances.
 */
@SuppressWarnings("checkstyle:TypeName")
public class CostFlywayCallbackTest extends Assert {

    private static final String TABLE_EXISTS_QUERY = "SELECT 1 FROM information_schema.tables " +
        "WHERE table_schema='TEST_DB' AND table_name='schema_version'";
    private static final String TEST_DB  = "TEST_DB";
    private Connection connection;
    private Statement statement;
    private static final String FLYWAY_TABLE_NAME = "schema_version";
    private static final String SAMPLE_VERSION = "1.26";
    private static final int SAMPLE_NEW_CHECKSUM = 546013560;
    private static final String MIGRATION_CORRECTION_PATH = "migration_correction_test.yaml";
    private static final String GET_CHECKSUM_QUERY = String.format(
            "SELECT checksum FROM %s WHERE version=%s",
            FLYWAY_TABLE_NAME, SAMPLE_VERSION);
    private static final String UPDATE_CHECKSUM_STMT = String.format(
            "UPDATE %s SET checksum = %d WHERE version='%s'",
            FLYWAY_TABLE_NAME, SAMPLE_NEW_CHECKSUM, SAMPLE_VERSION);
    private static final String SUCCESS_MIGRATION_STMT = String.format(
            "SELECT success FROM %s WHERE version=%s",
            FLYWAY_TABLE_NAME, SAMPLE_VERSION);
    private MigrationUpdate migrationUpdate;
    /**
     * Set up basic JDBC mocks.
     *
     * @throws SQLException must be declared but will not be thrown by our mocks
     */
    @Before
    public void setup() throws SQLException {
        this.connection = mock(Connection.class);
        this.statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        migrationUpdate = new Yaml().loadAs(getClass().getClassLoader()
                .getResourceAsStream(MIGRATION_CORRECTION_PATH), MigrationUpdates.class).getMigrationUpdates().get(0);
    }

    /**
     * Test scenario when there is no version table in the database.
     *
     * <p>This will happen when the database never been created by Flyway.</p>
     *
     * @throws SQLException must be declared but will not be thrown by our mocks
     */
    @Test
    public void testNoUpdateWhenNoVersionTable() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(CostFlywayCallback.GET_DATABASE_QUERY))
            .thenReturn(dbResults);
        // return the name of DB
        when(dbResults.getString(1)).thenReturn("TEST_DB");

        final ResultSet results = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(results);
        // no results from query to check if table exists
        when(results.next()).thenReturn(false);

        final CostFlywayCallback callback = new CostFlywayCallback();
        callback.validateAndMigrate(connection, migrationUpdate, TEST_DB);
        verifyStatementCounts(1, 0, 0);
    }

    /**
     * Test scenario when there's a version table but no record for our migration.
     *
     * <p>This will happen when on first launch after upgrade from a version that did not include
     * any V1.26 migration.</p>
     *
     * @throws SQLException must be declared but will not be thrown by our mocks
     */
    @Test
    public void testNoUpdateWhenVersionNotFound() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(CostFlywayCallback.GET_DATABASE_QUERY))
            .thenReturn(dbResults);
        // return the name of DB
        when(dbResults.getString(1)).thenReturn("TEST_DB");

        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);
        // non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);
        // but no record for our migration
        when(migrationResults.next()).thenReturn(false);

        final CostFlywayCallback callback = new CostFlywayCallback();
        callback.validateAndMigrate(connection, migrationUpdate, TEST_DB);
        verifyStatementCounts(1, 1, 0);
    }

    /**
     * Test scneario when there's a version table with a record for our migration that has the
     * correct checksum.
     *
     * <p>This will happen either after an upgrade from a version that had V1.26 to one with the
     * correct V1.26 migration, or after the incrrect V1.26 checksum has been corrected during
     * a prior launch.</p>
     *
     * @throws SQLException must be declared but will not be thrown by our mocks
     */
    @Test
    public void testNoUpdateWhenCorrectMigrationChecksum() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(CostFlywayCallback.GET_DATABASE_QUERY))
            .thenReturn(dbResults);
        // return the name of DB
        when(dbResults.getString(1)).thenReturn("TEST_DB");

        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);
        // non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);
        // non-empty result for our migration
        when(migrationResults.next()).thenReturn(false);
        // correct checksum returned
        when(migrationResults.getInt(1)).thenReturn(SAMPLE_NEW_CHECKSUM);

        final CostFlywayCallback callback = new CostFlywayCallback();
        callback.validateAndMigrate(connection, migrationUpdate, TEST_DB);
        verifyStatementCounts(1, 1, 0);

    }

    /**
     * Scenario when the version table exists and contains a V1.26 migration record with an
     * incorrect checksum.
     *
     * <p>This will happen after an upgrade from a version that contains the incorrect V1.26
     * migration</p>
     *
     * @throws SQLException must be declared but will never be thrown by our mocks
     */
    @Test
    public void testUpdateWhenIncorrectMigrationChecksum() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(CostFlywayCallback.GET_DATABASE_QUERY))
            .thenReturn(dbResults);
        // return the name of DB
        when(dbResults.getString(1)).thenReturn("TEST_DB");

        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);
        // non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(GET_CHECKSUM_QUERY))
                .thenReturn(migrationResults);
        // non-empty result for our migration
        when(migrationResults.next()).thenReturn(true);
        // incorrect checksum returned
        when(migrationResults.getInt(1)).thenReturn(SAMPLE_NEW_CHECKSUM - 1);

        when(migrationResults.getBoolean(1)).thenReturn(true);
        when(statement.executeQuery(SUCCESS_MIGRATION_STMT))
                .thenReturn(migrationResults);
        final CostFlywayCallback callback = new CostFlywayCallback();
        callback.validateAndMigrate(connection, migrationUpdate, TEST_DB);
        verifyStatementCounts(1, 1, 1);
    }

    private void verifyStatementCounts(int tblExistsCount, int getChecksumCount, int updateCount)
        throws SQLException {

        verify(statement, times(tblExistsCount)).executeQuery(TABLE_EXISTS_QUERY);
        verify(statement, times(getChecksumCount)).executeQuery(GET_CHECKSUM_QUERY);
        verify(statement, times(updateCount)).executeUpdate(UPDATE_CHECKSUM_STMT);
    }

}