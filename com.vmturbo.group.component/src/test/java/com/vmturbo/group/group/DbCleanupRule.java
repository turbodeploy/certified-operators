package com.vmturbo.group.group;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.Schema;
import org.junit.rules.ExternalResource;

/**
 * Rule to automatically cleanup a DB schema before the test.
 */
public class DbCleanupRule extends ExternalResource {
    private final DbConfigurationRule dbConfig;
    private final Schema schema;

    /**
     * Constructs DB cleanup rule.
     *
     * @param dbConfig DB configuration rule
     * @param schema schema to clean data in
     */
    public DbCleanupRule(@Nonnull DbConfigurationRule dbConfig, @Nonnull Schema schema) {
        this.dbConfig = Objects.requireNonNull(dbConfig);
        this.schema = Objects.requireNonNull(schema);
    }

    @Override
    protected void before() {
        dbConfig.clearData(schema);
    }
}
