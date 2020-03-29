package com.vmturbo.sql.utils.flyway;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

/**
 * Tests of the {@link ResetMigrationChecksumCallback} class.
 */
public class ResetMigrationChecksumCallbackTest extends CallbackTestBase {

    /**
     * Test that the callback is not invoked when there's no record of the migration to be reset.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testNoCallbackWhenRecordDoesNotExist() throws SQLException {
        when(resultSet.next()).thenReturn(true, false);
        final ResetMigrationChecksumCallback reset = spy(new ResetMigrationChecksumCallback(
                "1", ImmutableSet.of(), 1));
        reset.beforeValidate(connection);
        verify(reset, times(1)).isCallbackNeeded(connection);
        verify(reset, never()).performCallback(connection);
        verify(reset, never()).describeCallback();
    }

    /**
     * Test that the callback is not invoked when the migration's checksum is already correct.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testNoCallbackWhenChecksumIsCorrect() throws SQLException {
        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getInt(1)).thenReturn(1);
        final ResetMigrationChecksumCallback reset = spy(new ResetMigrationChecksumCallback(
                "1", ImmutableSet.of(), 1));
        reset.beforeValidate(connection);
        verify(reset, times(1)).isCallbackNeeded(connection);
        verify(reset, never()).performCallback(connection);
        verify(reset, never()).describeCallback();
    }

    /**
     * Test that the callback is not invoked when the migration's checksum is null and that is
     * the correct value.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testNoCallbackWhenChecksumIsCorrectNull() throws SQLException {
        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getObject(1)).thenReturn(null);
        final ResetMigrationChecksumCallback reset = spy(new ResetMigrationChecksumCallback(
                "1", ImmutableSet.of(), null));
        reset.beforeValidate(connection);
        verify(reset, times(1)).isCallbackNeeded(connection);
        verify(reset, never()).performCallback(connection);
        verify(reset, never()).describeCallback();
    }

    /**
     * Test that the callback is not called when the existing checksum is neither correct nor
     * listed among the expected prior checksums.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testNoCallbackWhenChecksumIsUnrecognized() throws SQLException {
        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getObject(1)).thenReturn(3);
        final ResetMigrationChecksumCallback reset = spy(new ResetMigrationChecksumCallback(
                "1", ImmutableSet.of(2), 1));
        reset.beforeValidate(connection);
        verify(reset, times(1)).isCallbackNeeded(connection);
        verify(reset, never()).performCallback(connection);
        verify(reset, never()).describeCallback();
    }

    /**
     * Test that the callback is invoked when the existing checksum is incorrect and is among
     * the expected prior checksums.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackCalledWhenRecognizedIncorrectChecksum() throws SQLException {
        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getObject(1)).thenReturn(2);
        final ResetMigrationChecksumCallback reset = spy(new ResetMigrationChecksumCallback(
                "1", ImmutableSet.of(2), 1));
        reset.beforeValidate(connection);
        verify(reset, times(1)).isCallbackNeeded(connection);
        verify(reset, times(1)).performCallback(connection);
        verify(reset, times(1)).describeCallback();
    }

    /**
     * Test that the callback is invoked if the existing checksum is null and null is not among
     * the expected prior values.
     *
     * @throws SQLException if there are DB problems
     */
    @Test
    public void testCallbackCalledWhenRecognizedIncorrectNull() throws SQLException {
        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getObject(1)).thenReturn(null);
        // ImmutableSet doesn't permit nulls
        Set<Integer> incorrectChecksums = new HashSet<>();
        incorrectChecksums.add(null);
        incorrectChecksums.add(2);
        final ResetMigrationChecksumCallback reset = spy(new ResetMigrationChecksumCallback(
                "1", incorrectChecksums, 1));
        reset.beforeValidate(connection);
        verify(reset, times(1)).isCallbackNeeded(connection);
        verify(reset, times(1)).performCallback(connection);
        verify(reset, times(1)).describeCallback();
    }

}
