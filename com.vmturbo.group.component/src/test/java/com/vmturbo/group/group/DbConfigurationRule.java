package com.vmturbo.group.group;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.junit.rules.ExternalResource;

import com.vmturbo.sql.utils.TestDbConfiguration;

/**
 * JUnit rule to create a workable DB connection for a component. Before the test Flyway migration
 * is applied, then Jooq DSL context is created. After the test, the DB is dropped.
 */
public class DbConfigurationRule extends ExternalResource {

    static {
        // Suppress JOOQ banner
        System.getProperties().setProperty("org.jooq.no-logo", "true");
    }

    private final TestDbConfiguration dbConfig;

    /**
     * Constructs the rule.
     *
     * @param originalSchemaName original schema name
     */
    public DbConfigurationRule(@Nonnull String originalSchemaName) {
        this.dbConfig = new TestDbConfiguration(originalSchemaName, "");
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
