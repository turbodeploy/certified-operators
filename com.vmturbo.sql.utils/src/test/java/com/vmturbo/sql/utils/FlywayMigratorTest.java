package com.vmturbo.sql.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.function.Supplier;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FlywayMigratorTest {

    final Flyway flyway = mock(Flyway.class);
    final Supplier<Flyway> flywayFactory = () -> flyway;

    final FlywayMigrator migrator = new FlywayMigrator(Duration.ofMillis(300),
        Duration.ofMillis(100),
        flywayFactory);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMigrateImmediateSuccess() throws Exception {
        assertEquals(flyway, migrator.migrate());
    }

    @Test
    public void testMigrateImmediateSuccessAfterOneFailure() throws Exception {
        when(flyway.migrate())
            .thenThrow(new FlywayException(FlywayMigrator.DATABASE_CONNECTION_FAILURE_MESSAGE))
            .thenReturn(1);

        assertEquals(flyway, migrator.migrate());
    }

    @Test
    public void testMigrationFailure() throws Exception {
        when(flyway.migrate()).thenThrow(new FlywayException("Bad migration"));

        expectedException.expect(FlywayException.class);
        expectedException.expectMessage("Bad migration");
        migrator.migrate();
    }

    @Test(timeout=30000) // Maximum duration of wait is 300 millis, test should definitely take less than 30 seconds
    public void testDatabaseConnectionUnreadyForDuration() throws Exception {
        when(flyway.migrate()).thenThrow(new FlywayException(FlywayMigrator.DATABASE_CONNECTION_FAILURE_MESSAGE));

        expectedException.expect(FlywayException.class);
        expectedException.expectMessage(FlywayMigrator.DATABASE_CONNECTION_FAILURE_MESSAGE);
        migrator.migrate();
    }
}