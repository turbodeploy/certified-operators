package com.vmturbo.sql.utils.flyway;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;

/**
 * Base class for migration callback tests.
 *
 * <p>This class just wires together some mocks that are used to mock database operations.</p>
 */
class CallbackTestBase {
    protected Connection connection;
    protected Statement statement;
    protected ResultSet resultSet;

    @Before
    public void before() throws SQLException {
        this.connection = mock(Connection.class);
        this.statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        this.resultSet = mock(ResultSet.class);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
    }
}
