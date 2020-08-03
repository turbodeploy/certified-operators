package com.vmturbo.sql.utils;

import java.time.Instant;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Schema;
import org.junit.rules.ExternalResource;

/**
 * JUnit rule to create a workable DB connection for a component. Before the test Flyway migration
 * is applied, then Jooq DSL context is created. After the test, the DB is dropped.
 *
 * <p/>The ideal use for this rule is at the class level:
 *
 * <pre>
 * {@code
 * public class MyTest {
 *     // This will create the database - specific to the test - and run migrations.
 *     @ClassRule
 *     public static DbConfigurationRule dbConfig = new DbConfigurationRule(Mydb.MYDB);
 *
 *     // This will clean up the database before every test.
 *     @Rule
 *     public DbCleanupRule dbCleanup = dbConfig.getCleanupRule();
 *
 *     private DSLContext dsl = dbConfig.getDslContext();
 * }
 * }
 * </pre>
 */
public class DbConfigurationRule extends ExternalResource {

    static {
        // Suppress JOOQ banner
        System.getProperties().setProperty("org.jooq.no-logo", "true");
    }

    private final TestDbConfiguration dbConfig;

    private final Schema schema;

    /**
     * Constructs the rule.
     *
     * @param originalSchema original schema name
     */
    public DbConfigurationRule(@Nonnull Schema originalSchema) {
        String testSchemaName = String.join("_", originalSchema.getName(), "test",
                String.valueOf(Instant.now().toEpochMilli()));
        this.dbConfig = new TestDbConfiguration(originalSchema, testSchemaName, "");
        this.schema = originalSchema;
    }

    /**
     * Get the {@link DbCleanupRule} that should be used as a JUnit rule in the test that uses
     * the {@link DbConfigurationRule} as a classrule.
     *
     * @return The {@link DbCleanupRule}.
     */
    @Nonnull
    public DbCleanupRule cleanupRule() {
        return new DbCleanupRule(this, schema);
    }

    @Override
    protected void before() {
        dbConfig.getFlyway().clean();
        dbConfig.getFlyway().migrate();
    }

    @Override
    protected void after() {
        dbConfig.getFlyway().clean();
    }

    /**
     * Returns Jooq DB context.
     *
     * @return DSL context
     */
    @Nonnull
    public DSLContext getDslContext() {
        return dbConfig.getDslContext();
    }
}
