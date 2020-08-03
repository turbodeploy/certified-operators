package com.vmturbo.sql.utils.flyway;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

/**
 * Tests of the {@link PreValidationMigraitonCallbackBaseTest} class.
 */
public class PreValidationMigraitonCallbackBaseTest extends CallbackTestBase {

    /**
     * Test that the callback is not exercised at all when the migrations table does not
     * exist.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackWithNoTable() throws SQLException {
        when(resultSet.next()).thenReturn(false);
        final NoopCallback noop = spy(new NoopCallback(false));
        noop.beforeValidate(connection);
        verify(noop, never()).isCallbackNeeded(connection);
        verify(noop, never()).performCallback(connection);
        // no log message if callback not needed
        verify(noop, never()).describeCallback();
    }

    /**
     * Test that the callback is asked whether it should execute and then does not execute if it
     * says no.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackWithTableAndUnneeded() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        final NoopCallback noop = spy(new NoopCallback(false));
        noop.beforeValidate(connection);
        verify(noop, times(1)).isCallbackNeeded(connection);
        verify(noop, never()).performCallback(connection);
        // no log message if callback not needed
        verify(noop, never()).describeCallback();
    }

    /**
     * Test that the callback is invoked if it says it is needed.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackWithTableAndNeeded() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        final NoopCallback noop = spy(new NoopCallback(true));
        noop.beforeValidate(connection);
        verify(noop, times(1)).isCallbackNeeded(connection);
        verify(noop, times(1)).performCallback(connection);
        // one log message before attempting callback
        verify(noop, times(1)).describeCallback();
    }

    /**
     * Test that the callback is not invoked if the check that the migrations table exists
     * throws an exception.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackTableCheckThrows() throws SQLException {
        when(resultSet.next()).thenThrow(new SQLException("Bam!"));
        final NoopCallback noop = spy(new NoopCallback(true));
        noop.beforeValidate(connection);
        verify(noop, never()).isCallbackNeeded(connection);
        verify(noop, never()).performCallback(connection);
        // one log message when table check throws
        verify(noop, times(1)).describeCallback();
    }

    /**
     * Test that if the callback throws an exception when asked whether the callback is needed,
     * it is not invoked.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackIsNeededThrows() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        final NoopCallback noop = spy(new NoopCallback(true));
        when(noop.isCallbackNeeded(connection)).thenThrow(new SQLException("Kapow!"));
        noop.beforeValidate(connection);
        verify(noop, times(1)).isCallbackNeeded(connection);
        verify(noop, never()).performCallback(connection);
        // one message is logged when exception is caught
        verify(noop, times(1)).describeCallback();
    }

    /**
     * Test that we get two log messages - one before the attempt and one after the failure -
     * if the callback throws an exception when it is invoked.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackPerformThrows() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        final NoopCallback noop = spy(new NoopCallback(true));
        doThrow(new SQLException("Kapow!")).when(noop).performCallback(connection);
        noop.beforeValidate(connection);
        verify(noop, times(1)).isCallbackNeeded(connection);
        verify(noop, times(1)).performCallback(connection);
        // in this case we get log messages before attempt and after catch
        verify(noop, times(2)).describeCallback();
    }


    /**
     * Simple test callback that does nothing if invoked.
     */
    private static class NoopCallback extends PreValidationMigraitonCallbackBase {

        private final boolean isNeeded;

        /**
         * Create a new instance.
         *
         * @param isNeeded whether this instance should say the callback is needed when asked
         */
        NoopCallback(boolean isNeeded) {
            this.isNeeded = isNeeded;
        }

        @Override
        protected void performCallback(final Connection connection) throws SQLException {
            // do nothing
        }

        @Override
        protected boolean isCallbackNeeded(final Connection connection) throws SQLException {
            return isNeeded;
        }

        @Override
        protected String describeCallback() {
            return "noop";
        }
    }
}
