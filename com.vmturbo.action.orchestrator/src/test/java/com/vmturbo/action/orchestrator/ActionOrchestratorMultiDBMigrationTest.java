package com.vmturbo.action.orchestrator;

import org.junit.Test;

import com.vmturbo.sql.utils.MultiDBMigrationTest;

/**
 * {@inheritDoc}.
 */
public class ActionOrchestratorMultiDBMigrationTest extends MultiDBMigrationTest {

    private static final int INITIAL_OFFSET = 24;
    private static final String DEFAULT_PATH = "src/main/resources/db/migration";
    private static final String MARIA_DB_PATH = "src/main/resources/db/migrations/action-orchestrator/mariadb";
    private static final String POSTGRES_PATH = "src/main/resources/db/migrations/action-orchestrator/postgres";

    /**
     * Test that the number of migrations for the supported databases match.
     */
    @Test
    public void testMigrations() {
        testMultiDBMigrations(INITIAL_OFFSET, DEFAULT_PATH, MARIA_DB_PATH, POSTGRES_PATH);
    }
}
