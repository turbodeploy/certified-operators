package com.vmturbo.sql.utils;

import java.io.File;
import java.util.Objects;

import org.junit.Assert;

/**
 * Ensures that each migration in the default folder has a corresponding migration for the supported
 * databases used by the DBEndpoint (currently MariaDB and Postgres). Since postgres was added much later
 * each component has an hardcoded offset that represents the number of already existing sql
 * migrations at the time the first Postgres migration was introduced.
 */
public abstract class MultiDBMigrationTest {

    /**
     * Test that the number of migrations for the supported databases match.
     * @param initialOffset number of sql migrations at the time the first postgres migration was introduced
     * @param defaultPath path with default migrations
     * @param mariaDBPath path with mariadb migrations
     * @param postgresPath path with postgres migrations
     */
    public void testMultiDBMigrations(final int initialOffset, final String defaultPath, final String mariaDBPath,
            final String postgresPath) {
        final File migrationDir = new File(defaultPath);
        final File mariaDbDir = new File(mariaDBPath);
        final File postgresDir = new File(postgresPath);


        Assert.assertEquals(Objects.requireNonNull(migrationDir.listFiles()).length, Objects.requireNonNull(
                mariaDbDir.listFiles()).length);
        Assert.assertEquals(Objects.requireNonNull(postgresDir.listFiles()).length + initialOffset, Objects.requireNonNull(
                mariaDbDir.listFiles()).length);
    }
}
