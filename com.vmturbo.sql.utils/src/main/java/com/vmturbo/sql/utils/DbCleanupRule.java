package com.vmturbo.sql.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Schema;
import org.jooq.Table;
import org.junit.rules.ExternalResource;

/**
 * Rule to automatically cleanup a DB schema before the test.
 */
public class DbCleanupRule extends ExternalResource {

    private static final Logger logger = LogManager.getLogger();

    private final DbConfigurationRule dbConfig;

    private final Schema schema;

    /**
     * Constructs DB cleanup rule.
     *
     * @param dbConfig DB configuration rule
     * @param schema schema to clean data in
     */
    DbCleanupRule(@Nonnull DbConfigurationRule dbConfig, @Nonnull Schema schema) {
        this.dbConfig = Objects.requireNonNull(dbConfig);
        this.schema = Objects.requireNonNull(schema);
    }

    /**
     * Create the cleanup queries - the queries we will run to delete all information from the
     * test database.
     *
     * @param schema The {@link Schema}.
     * @param context The context to use to generate {@link Query} objects.
     * @return A list of {@link Query} objects to execute to clean up the database.
     */
    private static List<Query> getCleanupQueries(@Nonnull final Schema schema,
                                                 @Nonnull final DSLContext context) {
        List<Table<?>> tablesWithViews = schema.getTables();
        List<Query> q = new ArrayList<>();
        // Disable foreign key checks so we can delete rows without worrying about order or
        // circular dependencies between tables.
        q.add(context.query("SET FOREIGN_KEY_CHECKS=0;"));
        tablesWithViews.stream()
            // Filter out views, because DELETE operations on views will cause errors.
            .filter(table -> !table.getComment().equals("VIEW"))
            .map(context::deleteFrom)
            .forEach(q::add);
        q.add(context.query("SET FOREIGN_KEY_CHECKS=1;"));
        return q;
    }

    /**
     * Clean all non-view tables in the provided database. This is used by this rule, but
     * doubles as a utility method. It should only be used in tests!
     *
     * @param context The {@link DSLContext} to use to execute the query.
     * @param schema The {@link Schema} object indicating the database to clean.
     */
    public static void cleanDb(@Nonnull final DSLContext context, @Nonnull final Schema schema) {
        logger.debug("Cleaning up schema {}", schema);
        // Run the cleanup queries in a batch. This saves time, because we just have one
        // round-trip to the database.
        final List<Query> cleanupQueries = getCleanupQueries(schema, context);
        final int[] modifiedRows = context.batch(cleanupQueries).execute();
        logger.trace("Query results: {}", () -> {
            final StringJoiner joiner = new StringJoiner(", ");
            for (int i = 0; i < cleanupQueries.size(); ++i) {
                joiner.add(cleanupQueries.get(i).getSQL())
                    .add(Integer.toString(modifiedRows[i]));
            }
            return joiner.toString();
        });
    }

    @Override
    protected void before() {
        final DSLContext context = dbConfig.getDslContext();
        try {
            cleanDb(context, schema);
        } catch (Exception e) {
            // If the cleanup fails for any reason, we will try to re-initialize the database
            // from scratch so that the database is fresh.
            logger.error(e);
            dbConfig.before();
        }
    }
}
