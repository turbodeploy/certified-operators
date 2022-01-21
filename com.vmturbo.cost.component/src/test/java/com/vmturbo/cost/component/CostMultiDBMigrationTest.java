package com.vmturbo.cost.component;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class CostMultiDBMigrationTest extends MultiDBMigrationTest {


    private static final int INITIAL_OFFSET = 75;
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/cost/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/cost/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Ignore
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }

}
