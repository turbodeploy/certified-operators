package com.vmturbo.market;

import org.junit.Test;

import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class MarketMultiDBMigrationTest extends MultiDBMigrationTest {

    private static final int INITIAL_OFFSET = 0;
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/market/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/market/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }
}
