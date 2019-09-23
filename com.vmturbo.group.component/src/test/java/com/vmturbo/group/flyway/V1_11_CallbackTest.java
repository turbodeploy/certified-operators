package com.vmturbo.group.flyway;

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
 * Test that the Flyway callback class to fix the V1.11 migration responds correctly
 * in all circumstances.
 */
@SuppressWarnings("checkstyle:TypeName")
public class V1_11_CallbackTest extends Assert {

    private Connection connection;
    private Statement statement;

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
        final ResultSet results = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.TABLE_EXISTS_QUERY))
            .thenReturn(results);
        // no results from query to check if table exists
        when(results.next()).thenReturn(false);

        final V1_11_Callback callback = new V1_11_Callback();
        callback.beforeValidate(connection);
        verifyStatementCounts(1, 0, 0);
    }

    /**
     * Test scenario when there's a version table but no record for our migration.
     *
     * <p>This will happen when on first launch after upgrade from a version that did not include
     * any V1.11 migration.</p>
     *
     * @throws SQLException must be declared but will not be thrown by our mocks
     */
    @Test
    public void testNoUpdateWhenVersionNotFound() throws SQLException {
        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);
        // non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);
        // but no record for our migration
        when(migrationResults.next()).thenReturn(false);

        final V1_11_Callback callback = new V1_11_Callback();
        callback.beforeValidate(connection);
        verifyStatementCounts(1, 1, 0);
    }

    /**
     * Test scneario when there's a version table with a record for our migration that has the
     * correct checksum.
     *
     * <p>This will happen either after an upgrade from a version that had V1.11 to one with the
     * correct V1.11 migration, or after the incrrect V1.11 checksum has been corrected during
     * a prior launch.</p>
     *
     * @throws SQLException must be declared but will not be thrown by our mocks
     */
    @Test
    public void testNoUpdateWhenCorrectMigrationChecksum() throws SQLException {
        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);
        // non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);
        // non-empty result for our migration
        when(migrationResults.next()).thenReturn(false);
        // correct checksum returned
        when(migrationResults.getInt(1)).thenReturn(V1_11_Callback.V1_11_CORRECT_CHECKSUM);

        final V1_11_Callback callback = new V1_11_Callback();
        callback.beforeValidate(connection);
        verifyStatementCounts(1, 1, 0);

    }

    /**
     * Scenario when the version table exists and contains a V1.11 migration record with an
     * incorrect checksum.
     *
     * <p>This will happen after an upgrade from a version that contains the incorrect V1.11
     * migration</p>
     *
     * @throws SQLException must be declared but will never be thrown by our mocks
     */
    @Test
    public void testUpdateWhenIncorrectMigrationChecksum() throws SQLException {
        final ResultSet tableResults = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.TABLE_EXISTS_QUERY))
            .thenReturn(tableResults);
        // non-empty results for table check
        when(tableResults.next()).thenReturn(true);

        final ResultSet migrationResults = mock(ResultSet.class);
        when(statement.executeQuery(V1_11_Callback.GET_CHECKSUM_QUERY))
            .thenReturn(migrationResults);
        // non-empty result for our migration
        when(migrationResults.next()).thenReturn(true);
        // incorrect checksum returned
        when(migrationResults.getInt(1)).thenReturn(V1_11_Callback.V1_11_CORRECT_CHECKSUM - 1);

        final V1_11_Callback callback = new V1_11_Callback();
        callback.beforeValidate(connection);
        verifyStatementCounts(1, 1, 1);
    }

    private void verifyStatementCounts(int tblExistsCount, int getChecksumCount, int updateCount)
        throws SQLException {

        verify(statement, times(tblExistsCount)).executeQuery(V1_11_Callback.TABLE_EXISTS_QUERY);
        verify(statement, times(getChecksumCount)).executeQuery(V1_11_Callback.GET_CHECKSUM_QUERY);
        verify(statement, times(updateCount)).executeUpdate(V1_11_Callback.UPDATE_CHECKSUM_STMT);
    }

}
