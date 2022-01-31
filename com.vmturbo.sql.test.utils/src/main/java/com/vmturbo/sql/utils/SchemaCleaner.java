package com.vmturbo.sql.utils;

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

import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrideInfo;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;

/**
 * Class that manages cleanup of a provisioned schema after each test execution.
 *
 * <p>This is currently used only with DbEndpoint scenarios, though similar functionality appears
 * in the legacy rules.</p>
 *
 * <p>Tables are selected for cleaning (via truncation) as follows:</p>
 *
 * <ul>
 *     <li>If a jOOQ listener active during the test notices an INSERT operation against a
 *     particular table, that table will be truncated.</li>
 *     <li>Reference tables are exempt from the above. These are tables that are found to be
 *     non-empty immediately after migrations are performed.</li>
 *     <li>Test classes and methods can optionally annotated using {@link CleanupOverrides} to
 *     add or remove specific tables from the list to be truncated.</li>
 * </ul>
 */
public class SchemaCleaner {
    private static final Logger logger = LogManager.getLogger();

    private final Schema schema;

    /**
     * Create a new instance for a given schema.
     *
     * @param schema the schema to monitor and clean
     */
    public SchemaCleaner(Schema schema) {
        this.schema = schema;
    }

    /**
     * Create a monitor to watch for inserts during a test, and then to kick off cleanups after the
     * test completes.
     *
     * @param testClass       test class containing the test
     * @param methodName      name of test being executed
     * @param referenceTables reference tables detected for this schema
     * @param baseTables      all base tables in this schema (omits views and other non-regular
     *                        tables)
     * @param dsl             DB access
     * @return a new monitor
     */
    public Monitor monitor(Class<?> testClass, String methodName, Set<Table<?>> referenceTables,
            Set<Table<?>> baseTables, DSLContext dsl) {
        return new Monitor(testClass, methodName, referenceTables, baseTables, dsl);
    }

    private void performCleanups(Class<?> testClass, String testMethodName,
            Set<Table<?>> insertedTables, Set<Table<?>> referenceTables,
            Set<Table<?>> baseTables, DSLContext dsl) {
        final Set<Class<?>> cleanups = insertedTables.stream().map(Object::getClass).collect(
                Collectors.toSet());
        if (cleanups.removeAll(
                referenceTables.stream().map(Object::getClass).collect(Collectors.toSet()))) {
            logger.warn("Some reference tables were the target of INSERT operations; "
                    + "not truncating {}", Sets.intersection(insertedTables, referenceTables));
        }
        final CleanupOverrideInfo overrides = new CleanupOverrideInfo(testClass, testMethodName);
        final SetView<Class<? extends Table<?>>> collisions = Sets.intersection(
                overrides.getTruncateTypes(), overrides.getRetainTypes());
        if (!collisions.isEmpty()) {
            logger.warn(
                    "@CleanupOverrides specified some tables for both truncation and retetnion; "
                            + "they will be retained: {}",
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
            if (overrides.isCheckOthers()) {
                nonTruncatedTables.stream()
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

        logger.log(level, "Cleanup: truncated {}; truncated for checkAll {}; failed truncation {}",
                sortTableNames(truncatedTables), sortTableNames(extraTruncatedTables),
                sortTableNames(failedTruncations));
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
                        dsl.truncateTable(t).execute();
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
        String sql;
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