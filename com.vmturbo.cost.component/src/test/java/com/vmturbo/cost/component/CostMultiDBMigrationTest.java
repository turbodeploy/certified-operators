package com.vmturbo.cost.component;

import org.junit.Test;

import com.vmturbo.commons.Pair;
import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class CostMultiDBMigrationTest extends MultiDBMigrationTest {

    /**
     * This initial offset represents the last mariadb migration that is included in the base migration for Postgres.
     * This is intended to NEVER be changed, with the exception of very few occasions. If that happens, we need to make sure
     * to update both the number, and put the new MariaDB migration name.
     */
    private static final Pair<Integer, String> SQL_INITIAL_OFFSET = new Pair<>(76, "V1_75__reorder_primary_keys_date_first.sql");
    private static final String SQL_DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String SQL_MARIA_DB_PATH = "src/main/resources/db/migrations/cost/mariadb";
    private static final String SQL_POSTGRES_PATH = "src/main/resources/db/migrations/cost/postgres";

    /**
     * This initial offset represents the last mariadb Java migration that was implemented before introducing Postgres.
     * It follows the same change policy as the above initial offset.
     */
    private static final Pair<Integer, String> JAVA_INITIAL_OFFSET = new Pair<>(0, "");
    private static final String JAVA_DEFAULT_PATH = "src/main/java/db/migration";
    private static final String JAVA_MARIA_DB_PATH = "src/main/java/db/migrations/cost/mariadb";
    private static final String JAVA_POSTGRES_PATH = "src/main/java/db/migrations/cost/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(SQL_INITIAL_OFFSET.first, SQL_DEFAULT_PATH, SQL_MARIA_DB_PATH,
                SQL_POSTGRES_PATH, JAVA_INITIAL_OFFSET.first, JAVA_DEFAULT_PATH, JAVA_MARIA_DB_PATH,
                JAVA_POSTGRES_PATH);
    }

}
