package com.vmturbo.history.schema;

import org.junit.Test;

import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class HistoryMultiDBMigrationTest extends MultiDBMigrationTest {


    private static final int INITIAL_OFFSET = 78;
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/history/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/history/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }

}
