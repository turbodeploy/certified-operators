package com.vmturbo.group.group;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.ForeignKey;
import org.jooq.Schema;
import org.jooq.Table;
import org.junit.rules.ExternalResource;

/**
 * Rule to automatically cleanup a DB schema before the test.
 */
public class DbCleanupRule extends ExternalResource {
    private final Logger logger = LogManager.getLogger();
    private final DbConfigurationRule dbConfig;
    private final Schema schema;
    private final List<Table<?>> tables;

    /**
     * Constructs DB cleanup rule.
     *
     * @param dbConfig DB configuration rule
     * @param schema schema to clean data in
     */
    public DbCleanupRule(@Nonnull DbConfigurationRule dbConfig, @Nonnull Schema schema) {
        this.dbConfig = Objects.requireNonNull(dbConfig);
        this.schema = Objects.requireNonNull(schema);
        final List<Table<?>> tablesList =
                CollectionUtils.sortWithDependencies(schema.getTables(), this::getReferences);
        Collections.reverse(tablesList);
        this.tables = Collections.unmodifiableList(tablesList);
        logger.trace("Tables will be cleaned up in the following order (reversed): {}", tables);
    }

    private Collection<Table<?>> getReferences(@Nonnull Table<?> src) {
        final Set<Table<?>> result = new HashSet<>();
        for (ForeignKey<?, ?> foreignKey : src.getReferences()) {
            result.add(foreignKey.getKey().getTable());
        }
        return result;
    }

    @Override
    protected void before() {
        final DSLContext context = dbConfig.getDslContext();
        logger.debug("Cleaning up schema {}", schema);
        for (Table<?> table: tables) {
            context.deleteFrom(table).execute();
        }
    }
}
