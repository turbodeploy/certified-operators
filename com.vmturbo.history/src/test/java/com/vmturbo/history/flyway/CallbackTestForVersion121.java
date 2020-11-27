package com.vmturbo.history.flyway;

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

/**
 * Flyway callback test class for fixing the V1.21 migration.
 */
public class CallbackTestForVersion121 extends Assert {

    private static final String TABLE_EXISTS_QUERY = "SELECT 1 FROM information_schema.tables "
            + "WHERE table_schema='TEST_DB' AND table_name='schema_version'";
    private static final String TEST_DB  = "TEST_DB";
    private Connection connection;
    private Statement statement;

    /**
     * Set up the JDBC mocks.
     *
     * @throws SQLException must be declared but will not be thrown by the mocks.
     */
    @Before
    public void setup() throws SQLException {
        this.connection = mock(Connection.class);
        this.statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
    }

    /**
     * Test scenario when there is no version table in the database.
     *
     * <p>This will happen when the database never been created by Flyway.</p>
     *
     * @throws SQLException must be declared but will not be thrown by the mocks.
     */
    @Test
    public void testNoUpdateWhenNoVersionTable() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_DATABASE_QUERY))
            .thenReturn(dbResults);

        when(dbResults.getString(1)).thenReturn(TEST_DB);

        final ResultSet results = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(results);
        when(results.next()).thenReturn(false);

        final MigrationCallbackForVersion121 callback = new MigrationCallbackForVersion121();
        callback.beforeValidate(connection);

        verifyStatementCounts(1, 0, 0);
    }

    /**
     * Test scenario when there's a version table but no record for the V1.21 migration.
     *
     * @throws SQLException must be declared but will not be thrown.
     */
    @Test
    public void testNoUpdateWhenVersionNotFound() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_DATABASE_QUERY))
            .thenReturn(dbResults);

        when(dbResults.getString(1)).thenReturn(TEST_DB);

        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);

        // Non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);

        // No record for the migration has been found
        when(migrationResults.next()).thenReturn(false);

        final MigrationCallbackForVersion121 callback = new MigrationCallbackForVersion121();
        callback.beforeValidate(connection);

        verifyStatementCounts(1, 1, 0);
    }

    /**
     * Test when there's a version table with a record for our migration that has the
     * correct checksum.
     *
     * <p>This can happen either after an upgrade from a version that had V1.21 to one with the
     * correct V1.21 migration, or after the incorrect V1.21 checksum has been fixed during
     * a prior launch.</p>
     *
     * @throws SQLException must be declared but will not be thrown.
     */
    @Test
    public void testNoUpdateWhenCorrectMigrationChecksum() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_DATABASE_QUERY))
            .thenReturn(dbResults);

        when(dbResults.getString(1)).thenReturn(TEST_DB);

        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);

        // Non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);

        // Record exists for the migration
        when(migrationResults.next()).thenReturn(false);

        // Correct checksum returned
        when(migrationResults.getInt(1)).thenReturn(MigrationCallbackForVersion121.V1_21_CORRECT_CHECKSUM);

        final MigrationCallbackForVersion121 callback = new MigrationCallbackForVersion121();
        callback.beforeValidate(connection);

        verifyStatementCounts(1, 1, 0);
    }

    /**
     * Scenario when the version table exists and contains a V1.21 migration record with an
     * incorrect checksum.
     *
     * <p>This will happen after an upgrade from a version that contains the incorrect V1.21
     * migration.</p>
     *
     * @throws SQLException must be declared but will never be thrown.
     */
    @Test
    public void testUpdateWhenIncorrectMigrationChecksum() throws SQLException {
        final ResultSet dbResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_DATABASE_QUERY))
            .thenReturn(dbResults);

        when(dbResults.getString(1)).thenReturn(TEST_DB);

        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);

        // Non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(MigrationCallbackForVersion121.GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);

        // Record exists for the migration
        when(migrationResults.next()).thenReturn(true);

        // Incorrect checksum is returned
        when(migrationResults.getInt(1)).thenReturn(MigrationCallbackForVersion121.V1_21_CORRECT_CHECKSUM - 1);

        final MigrationCallbackForVersion121 callback = new MigrationCallbackForVersion121();
        callback.beforeValidate(connection);
        verifyStatementCounts(1, 1, 1);
    }

    private void verifyStatementCounts(int tblExistsCount, int getChecksumCount, int updateCount)
        throws SQLException {

        verify(statement, times(tblExistsCount)).executeQuery(TABLE_EXISTS_QUERY);
        verify(statement, times(getChecksumCount)).executeQuery(MigrationCallbackForVersion121.GET_CHECKSUM_QUERY);
        verify(statement, times(updateCount)).executeUpdate(MigrationCallbackForVersion121.UPDATE_CHECKSUM_STMT);
    }

}
