package com.vmturbo.action.orchestrator;

import org.junit.Test;

import com.vmturbo.commons.Pair;
import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class ActionOrchestratorMultiDBMigrationTest extends MultiDBMigrationTest {

    /**
     * This initial offset represents the last mariadb migration that is included in the base migration for Postgres.
     * This is intended to NEVER be changed, with the exception of very few occasions. If that happens, we need to make sure
     * to update both the number, and put the new MariaDB migration name.
     */
    private static final Pair<Integer, String> INITIAL_OFFSET = new Pair<>(24, "V1_24__workflow_primary_key.sql");
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/action-orchestrator/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/action-orchestrator/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET.first, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }
}
