package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.jooq.Clause;
import org.jooq.DSLContext;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.VisitContext;
import org.jooq.VisitListenerProvider;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultVisitListener;
import org.jooq.impl.DefaultVisitListenerProvider;
import org.junit.Assert;
import org.junit.runner.Description;

import com.vmturbo.common.api.utils.EnvironmentUtils;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrideInfo;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class that manages cleanup of a provisioned schema after each test execution.
 *
 * <p>This is currently by DbEndpoint and legacy tests (`DbCleanupRule` now conses up one of
 * these.)
 *
 * <p>Tables are selected for cleaning (via truncation) as follows:</p>
 *
 * <ul>
 *     <li>If a jOOQ listener active during the test notices an INSERT operation against a
 *     particular table, that table will be truncated.</li>
 *     <li>Reference tables are exempt from the above. These are tables that are found to be
 *     non-empty immediately after migrations are performed.</li>
 *     <li>Test classes and methods can optionally be annotated using {@link CleanupOverrides} to
 *     add or remove specific tables from the list to be truncated.</li>
 * </ul>
 *
 * <p>By default, tables that are neither reference tables nor already selected for truncation by
 * above rules are checked to make sure they're empty. Any violations will cause the test to fail
 * because it is leaking table data that could could cause other tests to fail mysteriously.</p>
 *
 * <p>The table scan can be suppressed by setting `SKIP_SCHEMA_CLEANUP_TALBLE_SCAN=true` either as
 * a system property or an environment variable. This speeds casual builds, but it is not reliable
 * for pre-merge testing. Failures can be addressed by adding `@CleanupOverrides` annotations.</p>
 */
public class SchemaCleaner {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Set this system property or environment variable to "true" to avoid scanning for empty
     * tables.
     */
    public static final String SKIP_SCHEMA_CLEANUP_TABLE_SCAN = "SKIP_SCHEMA_CLEANUP_TABLE_SCAN";

    private final Schema schema;
    private final DSLContext dsl;
    private final Set<Table<?>> baseTables;
    private final Set<Table<?>> refTables;

    /**
     * Create a new instance for a given schema.
     *
     * @param schema the schema to monitor and clean
     * @param dsl    database access
     */
    SchemaCleaner(Schema schema, DSLContext dsl) {
        this.schema = schema;
        this.dsl = dsl;
        this.baseTables = discoverBaseTables();
        this.refTables = discoverReferenceTables();
    }

    /**
     * Create a monitor to watch for inserts during a test, and then to kick off cleanups after the
     * test completes.
     *
     * @param description {@link Description} provided by the test runner, with info about the test
     * @return a new monitor
     */
    public Monitor monitor(Description description) {
        return new Monitor(description.getTestClass(),
                getRealMethodName(description.getMethodName()),
                refTables, baseTables, dsl);
    }

    private void performCleanups(Class<?> testClass, String testMethodName,
            Set<Table<?>> insertedTables, Set<Table<?>> referenceTables,
            Set<Table<?>> baseTables, DSLContext dsl) {
        final boolean scanTables =
                !EnvironmentUtils.parseBooleanFromEnv(SKIP_SCHEMA_CLEANUP_TABLE_SCAN);
        final String testName = String.format("[%s#%s]", testClass.getSimpleName(), testMethodName);
        final Set<Class<?>> cleanups = insertedTables.stream().map(Object::getClass).collect(
                Collectors.toSet());
        if (cleanups.removeAll(
                referenceTables.stream().map(Object::getClass).collect(Collectors.toSet()))) {
            logger.warn("{}: Some reference tables were the target of INSERT operations; "
                            + "not truncating {}", testName,
                    Sets.intersection(insertedTables, referenceTables));
        }
        final CleanupOverrideInfo overrides = new CleanupOverrideInfo(testClass, testMethodName);
        final SetView<Class<? extends Table<?>>> collisions = Sets.intersection(
                overrides.getTruncateTypes(), overrides.getRetainTypes());
        if (!collisions.isEmpty()) {
            logger.warn(
                    "{}: @CleanupOverrides specified some tables for both truncation and retetnion; "
                            + "they will be retained: {}", testName,
                    collisions.stream().map(Class::getSimpleName).collect(Collectors.toList()));
        }
        cleanups.addAll(overrides.getTruncateTypes());
        cleanups.removeAll(overrides.getRetainTypes());
        final Set<Table<?>> truncatedTables = new HashSet<>();
        final Set<Table<?>> failedTruncations = new HashSet<>();
        final Set<Table<?>> nonTruncatedTables = new HashSet<>();
        final Set<Table<?>> extraTruncatedTables = new HashSet<>();
        schema.tableStream()
                .filter(baseTables::contains)
                .forEach(nonTruncatedTables::add);
        nonTruncatedTables.stream()
                .filter(t -> cleanups.contains(t.getClass()))
                .forEach(truncatedTables::add);
        nonTruncatedTables.removeAll(truncatedTables);
        dsl.transaction(trans -> {
            DSLContext tdsl = DSL.using(trans);
            setForeignKeyChecks(false, truncatedTables, tdsl);
            failedTruncations.addAll(tryTruncate(truncatedTables, tdsl));
            truncatedTables.removeAll(failedTruncations);
            if (scanTables) {
                nonTruncatedTables.stream()
                        .filter(t -> !referenceTables.contains(t))
                        .filter(tdsl::fetchExists)
                        .forEach(extraTruncatedTables::add);
                tryTruncate(extraTruncatedTables, tdsl).forEach(t -> {
                    failedTruncations.add(t);
                    extraTruncatedTables.remove(t);
                });
            }
            setForeignKeyChecks(true, truncatedTables, tdsl);
        });

        Level level = extraTruncatedTables.isEmpty() && failedTruncations.isEmpty()
                      ? Level.DEBUG
                      : Level.WARN;
        logger.log(level,
                "{}: Cleanup truncated {}; truncated for checkAll {}; failed truncation {}",
                testName, sortTableNames(truncatedTables), sortTableNames(extraTruncatedTables),
                sortTableNames(failedTruncations));
        if (scanTables && !extraTruncatedTables.isEmpty()) {
            Assert.fail(String.format("Table insertions leaked from tables %s in test %s;"
                            + " @CleanupOverrides can be used to close these leaks",
                    extraTruncatedTables, testName));
        }
    }

    private Collection<Table<?>> tryTruncate(Collection<Table<?>> tables, DSLContext dsl) {
        if (tables.isEmpty()) {
            return tables;
        }
        switch (dsl.dialect()) {
            case MARIADB:
            case MYSQL:
                List<Table<?>> failures = new ArrayList<>();
                for (Table<?> t : tables) {
                    try {
                        // note that we must use TRUCATE rather than DELETE in order to ensure
                        // that auto-increment value are reset; we have tests that fail otherwise
                        dsl.truncate(t).execute();
                    } catch (DataAccessException e) {
                        failures.add(t);
                    }
                }
                return failures;
            case POSTGRES:
                String sql = String.format("TRUNCATE TABLE %s CASCADE",
                        tables.stream().map(Table::getName).map(Strings::dquote)
                                .collect(Collectors.joining(",")));
                try {
                    dsl.execute(sql);
                    return Collections.emptyList();
                } catch (DataAccessException e) {
                    return tables;
                }
            default:
                return tables;
        }
    }

    private List<String> sortTableNames(Collection<Table<?>> tables) {
        return tables.stream()
                .map(Table::getName)
                .sorted()
                .collect(Collectors.toList());
    }

    private void setForeignKeyChecks(boolean enabled, Collection<Table<?>> tables, DSLContext
            dsl) {
        switch (dsl.dialect()) {
            case MARIADB:
            case MYSQL:
                dsl.execute(String.format("SET foreign_key_checks = %d", enabled ? 1 : 0));
                break;
            default:
                break;
        }
    }

    /**
     * This deals with an issue with parameterized tests, wherein the "test method name" provided R
     * to rules is not really the method name, because it includes rendered parameter values
     * following the method name in square brackets. We need the true method name to look up
     * annotations.
     *
     * @param methodName method name provided by JUnit
     * @return method name with parameters stripped
     */
    private String getRealMethodName(String methodName) {
        int bracket = methodName.indexOf('[');
        return bracket >= 0 ? methodName.substring(0, bracket) : methodName;
    }

    /**
     * Find reference tables. This is used immediately after initial provisioning, by searching for
     * tables that are not empty. All such tables are considered to hold reference data that should
     * be preserved, so the tables will be exempt from cleaning.
     *
     * @return reference tables
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    private Set<Table<?>> discoverReferenceTables() {
        return schema.tableStream()
                .filter(baseTables::contains)
                .filter(dsl::fetchExists)
                .collect(Collectors.toSet());
    }

    /**
     * Collect all the base tables (not views or other varieties of table) in our schema.
     *
     * @return base tables
     */
    private Set<Table<?>> discoverBaseTables() {
        Set<String> baseTableNames = getBaseTableNames();
        return schema.tableStream()
                .filter(t -> baseTableNames.contains(t.getName()))
                .collect(Collectors.toSet());
    }

    /**
     * Collect the names of all base tables in the schema, by actually querying the database. This
     * is needed because jOOQ's in-memory model cannot distinguish base tables from views and other
     * table-ish objets.
     *
     * @return names of base tables
     */
    private Set<String> getBaseTableNames() {
        String sql;
        try {
            return InformationSchema.getTables(schema, dsl).stream()
                    .map(Table::getName)
                    .collect(Collectors.toSet());
        } catch (DataAccessException e) {
            logger.error("Failed to obtain list of base tables from DB; "
                    + "using jOOQ metadata for all tables", e);
            return schema.tableStream().map(Table::getName).collect(Collectors.toSet());
        }
    }

    /**
     * Class for monitoring a test execution. We install a jOOQ listener to watch for INSERT
     * operations and capture the target tables.
     *
     * <p>The monitor is autoclosable, and closing kicks off cleaning of the associated
     * schemas.</p>
     */
    class Monitor implements AutoCloseable {
        private final DSLContext dsl;
        private final Class<?> testClass;
        private final String methodName;
        private final Set<Table<?>> referenceTables;
        private final Set<Table<?>> baseTables;
        Set<Table<?>> insertedTables = new HashSet<>();

        /**
         * Create a new instance.
         *
         * @param testClass       test class
         * @param methodName      name of test method
         * @param referenceTables name of test method
         * @param baseTables      all base tables in associated schema
         * @param dsl             DB access
         */
        Monitor(Class<?> testClass, String methodName, Set<Table<?>> referenceTables,
                Set<Table<?>> baseTables, DSLContext dsl) {
            this.testClass = testClass;
            this.methodName = methodName;
            this.referenceTables = referenceTables;
            this.baseTables = baseTables;
            this.dsl = dsl;
            VisitListenerProvider provider = new DefaultVisitListenerProvider(
                    new InsertVisitListener());
            dsl.configuration().set(provider);
        }

        @Override
        public void close() {
            // done with test, kick off cleanups
            performCleanups(testClass, methodName, insertedTables, referenceTables, baseTables,
                    dsl);
        }

        /**
         * Visitor class to watch for INSERT operations and record target tables.
         */
        private class InsertVisitListener extends DefaultVisitListener {
            @Override
            public void visitEnd(VisitContext context) {
                super.visitEnd(context);
                if (context.clause() == Clause.INSERT) {
                    context.data().keySet().stream()
                            .filter(k -> k instanceof Enum<?> && k.toString()
                                    .equals("DATA_DML_TARGET_TABLE"))
                            .findFirst()
                            .ifPresent(targetTableKey -> {
                                Object table = context.data().get(targetTableKey);
                                insertedTables.add((Table<?>)table);
                            });
                }
            }
        }
    }
}