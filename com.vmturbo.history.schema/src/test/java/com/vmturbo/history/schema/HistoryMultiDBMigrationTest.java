package com.vmturbo.history.schema;

import org.junit.Test;

import com.vmturbo.commons.Pair;
import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class HistoryMultiDBMigrationTest extends MultiDBMigrationTest {


    /**
     * This initial offset represents the last mariadb migration that is included in the base migration for Postgres.
     * This is intended to NEVER be changed, with the exception of very few occasions. If that happens, we need to make sure
     * to update both the number, and put the new MariaDB migration name.
     */
    private static final Pair<Integer, String> INITIAL_OFFSET = new Pair<>(80, "V1.61__fix_cnt_indexes.sql");
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/history/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/history/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET.first, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }

}
