package com.vmturbo.sql.utils.flyway;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.junit.Test;

/**
 * Test class for {@link ForgetMigrationCallback} class.
 */
public class ForgetMigrationCallbackTest extends CallbackTestBase {

    /**
     * Test that the callback is performed when the migration appears in the migration history.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackPerformedWhenRecordIsPresent() throws SQLException {
        when(resultSet.next()).thenReturn(true, true);
        final ForgetMigrationCallback forgetter = spy(new ForgetMigrationCallback("1"));
        forgetter.beforeValidate(connection);
        verify(forgetter, times(1)).isCallbackNeeded(connection);
        verify(forgetter, times(1)).performCallback(connection);
        verify(forgetter, times(1)).describeCallback();
    }

    /**
     * Test that the callback is not invoked when the migration does not appear in the
     * migration history.
     *
     * @throws SQLException if there are DB issues
     */
    @Test
    public void testCalbackNotPerformedWhenRecordIsAbsent() throws SQLException {
        when(resultSet.next()).thenReturn(true, false);
        final ForgetMigrationCallback forgetter = spy(new ForgetMigrationCallback("1"));
        forgetter.beforeValidate(connection);
        verify(forgetter, times(1)).isCallbackNeeded(connection);
        verify(forgetter, never()).performCallback(connection);
        verify(forgetter, never()).describeCallback();
    }
}
