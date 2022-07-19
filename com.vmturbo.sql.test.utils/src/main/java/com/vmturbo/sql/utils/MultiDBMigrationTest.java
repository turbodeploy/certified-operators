package com.vmturbo.sql.utils;

import java.io.File;
import java.util.Objects;

import org.junit.Assert;

/**
 * Ensures that each migration in the default folder has a corresponding migration for the supported
 * databases used by the DBEndpoint (currently MariaDB and Postgres). Since postgres was added much later
 * each component has a hardcoded offset that represents the number of already existing sql
 * migrations at the time the first Postgres migration was introduced.
 */
public abstract class MultiDBMigrationTest {

    /**
     * Test that the number of migrations for the supported databases match.
     * @param sqlInitialOffset number of sql migrations at the time the first postgres migration was introduced
     * @param sqlDefaultPath path with default sql migrations
     * @param sqlMariaDBPath path with mariadb sql migrations
     * @param sqlPostgresPath path with postgres sql migrations
     * @param javaInitialOffset number of java migrations at the time the first postgres migration was introduced
     * @param javaDefaultPath path with default java migrations
     * @param javaMariaDBPath path with mariadb java migrations
     * @param javaPostgresPath path with postgres java migrations
     */
    public void testMultiDBMigrations(final int sqlInitialOffset, final String sqlDefaultPath, final String sqlMariaDBPath,
            final String sqlPostgresPath, final int javaInitialOffset, final String javaDefaultPath, final String javaMariaDBPath,
            final String javaPostgresPath) {
        Assert.assertEquals(filesCount(sqlDefaultPath), filesCount(sqlMariaDBPath));
        Assert.assertEquals(filesCount(sqlPostgresPath) + sqlInitialOffset, filesCount(sqlMariaDBPath));
        Assert.assertEquals(filesCount(javaDefaultPath), filesCount(javaMariaDBPath));
        Assert.assertEquals(filesCount(javaMariaDBPath), filesCount(javaPostgresPath) + javaInitialOffset);
    }

    private static int filesCount(final String path) {
        final File folder = new File(path);
        // If the folder doesn't exist, the count of the migrations is zero.
        if (!folder.exists()) {
            return 0;
        }
        return Objects.requireNonNull(folder.listFiles()).length;
    }
}
