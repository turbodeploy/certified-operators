package com.vmturbo.auth.component;

import org.junit.Test;

import com.vmturbo.commons.Pair;
import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class AuthMultiDBMigrationTest extends MultiDBMigrationTest {

    /**
     * This initial offset represents the last mariadb migration that is included in the base migration for Postgres.
     * This is intended to NEVER be changed, with the exception of very few occasions. If that happens, we need to make sure
     * to update both the number, and put the new MariaDB migration name.
     */
    private static final Pair<Integer, String> INITIAL_OFFSET = new Pair<>(1, "V1_1__create_widgetset_table.sql");
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/auth/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/auth/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET.first, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }

}
