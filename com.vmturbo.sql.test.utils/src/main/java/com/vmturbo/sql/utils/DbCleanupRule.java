package com.vmturbo.sql.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Clause;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.VisitContext;
import org.jooq.impl.DefaultVisitListener;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to automatically cleanup a DB schema before the test.
 */
public class DbCleanupRule implements TestRule {

    private static final Logger logger = LogManager.getLogger();

    private final DbConfigurationRule dbConfig;

    private final Schema schema;

    private boolean initializedTablesDiscovered = false;

    private final Set<Class<? extends Table<?>>> tableTypesWithInserts = new HashSet<>();

    /**
     * Constructs DB cleanup rule.
     *
     * @param dbConfig DB configuration rule
     * @param schema   schema to clean data in
     */
    DbCleanupRule(@Nonnull DbConfigurationRule dbConfig, @Nonnull Schema schema) {
        this.dbConfig = Objects.requireNonNull(dbConfig);
        this.schema = Objects.requireNonNull(schema);
        dbConfig.getDslContext().configuration().set(() -> new DefaultVisitListener() {
            @Override
            public void visitEnd(VisitContext context) {
                super.visitEnd(context);
                if (context.clause() == Clause.INSERT) {
                    context.data().keySet().stream()
                            .filter(k -> k instanceof Enum<?> && k.toString()
                                    .equals("DATA_DML_TARGET_TABLE"))
                            .findFirst()
                            .ifPresent(targetTableKey -> tableTypesWithInserts.add(
                                    (Class<? extends Table<?>>)context.data()
                                            .get(targetTableKey).getClass()));
                }
            }
        });
    }

    /**
     * Create the cleanup queries - the queries we will run to delete all information from the
     * test database.
     *
     * @param schema      The {@link Schema}.
     * @param dsl         The context to use to generate {@link Query} objects.
     * @param retained    Tables that should not be truncated, since they contain data
     *                    inserted by migrations
     * @param truncated   Tables that should be truncated even if we didn't detect any insertions
     * @param checkOthers check for non-empty tables that would not normally be checked; this
     *                    is generally used temporarily to figure out tables that should be added
     *                    to truncated list
     * @return A list of {@link Query} objects to execute to clean up the database.
     */
    private List<Query> getCleanupQueries(@Nonnull final Schema schema,
            @Nonnull final DSLContext dsl, Set<Class<? extends Table<?>>> retained,
            Set<Class<? extends Table<?>>> truncated, boolean checkOthers) {
        List<Table<?>> tablesWithViews = schema.getTables();
        List<Query> q = new ArrayList<>();
        // Disable foreign key checks so we can delete rows without worrying about order or
        // circular dependencies between tables.
        q.add(dsl.query("SET FOREIGN_KEY_CHECKS=0;"));
        tablesWithViews.stream()
                // Filter out views, because DELETE operations on views will cause errors.
                .filter(table -> !Objects.equals(table.getComment(), "VIEW"))
                .filter(t -> !retained.contains(t))
                .filter(t -> tableTypesWithInserts.contains(t.getClass())
                        || truncated.contains(t.getClass())
                        || (checkOthers && dsl.fetchExists(t)))
                .peek(t -> logger.info("Truncating table {} during cleanup", t))
                .map(dsl::truncate)
                .forEach(q::add);
        q.add(dsl.query("SET FOREIGN_KEY_CHECKS=1;"));
        return q;
    }

    /**
     * Clean all non-view tables in the provided database. This is used by this rule, but
     * doubles as a utility method. It should only be used in tests!
     *
     * @param context      The {@link DSLContext} to use to execute the query.
     * @param schema       The {@link Schema} object indicating the database to clean.
     * @param exemptTables Tables that should not be cleaned because they have data that was
     *                     inserted during migrations
     * @param overrideInfo information gathered from relevant @CleanupOverrides annotations
     */
    public void cleanDb(@Nonnull final DSLContext context, @Nonnull final Schema schema,
            Set<Class<? extends Table<?>>> exemptTables, CleanupOverrideInfo overrideInfo) {
        logger.debug("Cleaning up schema {}", schema);
        // Run the cleanup queries in a batch. This saves time, because we just have one
        // round-trip to the database.
        final List<Query> cleanupQueries = getCleanupQueries(schema, context,
                Sets.union(exemptTables, overrideInfo.getRetainTypes()),
                overrideInfo.getTruncateTypes(), overrideInfo.isCheckOthers());
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

    /**
     * Get a list of all the tables that have data. This is used after initial migration in order
     * to identify tables that should not be cleared during cleanup.
     *
     * @param schema DB schema
     * @param dsl    DB access
     * @return tables that are not empty
     */
    public Set<Class<? extends Table<?>>> discoverInitializedTables(@Nonnull final Schema schema,
            @Nonnull final DSLContext dsl) {
        if (initializedTablesDiscovered) {
            // Filter out views, because DELETE operations on views will cause errors.
            Set<Class<? extends Table<?>>> initiallyNonEmptyTables = schema.getTables().stream()
                    // Filter out views, because DELETE operations on views will cause errors.
                    .filter(table -> !table.getComment().equals("VIEW"))
                    .filter(dsl::fetchExists)
                    .map(Table::getClass)
                    .map(cls -> (Class<? extends Table<?>>)cls)
                    .collect(Collectors.toSet());
            initializedTablesDiscovered = true;
            return initiallyNonEmptyTables;
        } else {
            // we end up getting called once before we've done any migrations, so that first time
            // we'll proceed with an empty set, which is fine since at that point the whole
            // database is empty
            return Collections.emptySet();
        }
    }

    protected void before() {
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {
                    final DSLContext dsl = dbConfig.getDslContext();
                    cleanDb(dsl, schema, discoverInitializedTables(schema, dsl),
                            new CleanupOverrideInfo(description.getTestClass(),
                                    description.getMethodName()));
                }
            }
        };
    }

    /**
     * Class to combine class-level and method-level info from @CleanupOverrides annotations.
     */
    static class CleanupOverrideInfo {
        private final Set<Class<? extends Table<?>>> truncateTypes;
        private final Set<Class<? extends Table<?>>> retainTypes;
        private final boolean checkOthers;

        /**
         * Create a new instance.
         *
         * @param testClass      test class
         * @param testMethodName name of test method
         */
        CleanupOverrideInfo(Class<?> testClass, String testMethodName) {
            final CleanupOverrides classLevel = testClass.getAnnotation(CleanupOverrides.class);
            CleanupOverrides methodLevel = null;
            try {
                Method method = testClass.getMethod(testMethodName);
                methodLevel = method.getAnnotation(CleanupOverrides.class);
            } catch (NoSuchMethodException e) {
                logger.warn("Failed to check CleanupOverrides annotation for test method {}#{}",
                        testClass.getName(), testMethodName);
            }

            this.truncateTypes = new HashSet<>();
            this.retainTypes = new HashSet<>();

            boolean checkOthers = false;
            if (classLevel != null) {
                truncateTypes.addAll(Arrays.asList(classLevel.truncate()));
                retainTypes.addAll(Arrays.asList(classLevel.retain()));
                checkOthers = classLevel.checkOthers();
            }
            if (methodLevel != null) {
                truncateTypes.addAll(Arrays.asList(methodLevel.truncate()));
                retainTypes.addAll(Arrays.asList(methodLevel.retain()));
                checkOthers = checkOthers || methodLevel.checkOthers();
            }
            this.checkOthers = checkOthers;
        }

        public Set<Class<? extends Table<?>>> getTruncateTypes() {
            return truncateTypes;
        }

        public Set<Class<? extends Table<?>>> getRetainTypes() {
            return retainTypes;
        }

        public boolean isCheckOthers() {
            return checkOthers;
        }
    }

    /**
     * Annotation to customize cleanup processing for a given test or test class.
     *
     * <p>Normally, after every test, the {@link DbCleanupRule} truncates tables for which INSERT
     * opreations were detedcted during the test, and which were empty after the initial migration.
     * In some cases, the list of truncated tables needs to be adjusted. For example, tables
     * populated by stored procs may not be included automatically.</p>
     *
     * <p>Tables listed in the {@link #retain()} list will not be truncated. Those listed in
     * {@link #truncate()} will be.</p>
     *
     * <p>If {@link #checkOthers()} is true, all tables will be checked, and truncated if not
     * empty. This is mostly intended for temporary use in order to discover tables that may need
     * to be added ot the {@link #truncate()} list.e</p>
     */
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface CleanupOverrides {
        /**
         * Specify tables to be retained (not truncated).
         *
         * @return retained tables
         */
        Class<? extends Table<?>>[] retain() default {};

        /**
         * Specify tables to be truncated.
         *
         * @return truncated tables
         */
        Class<? extends Table<?>>[] truncate() default {};

        /**
         * Indicate that all tables should be checked for data and truncated if not empty.
         * @return true to check all tables
         */
        boolean checkOthers() default false;
    }
}
