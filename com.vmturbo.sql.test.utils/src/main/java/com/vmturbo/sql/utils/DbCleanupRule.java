package com.vmturbo.sql.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Schema;
import org.jooq.Table;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to automatically cleanup a DB schema before the test.
 */
public class DbCleanupRule implements TestRule {

    private static final Logger logger = LogManager.getLogger();

    private final Schema schema;
    private final DSLContext dsl;
    private final SchemaCleaner schemaCleaner;
    private List<Table<?>> existingTables;

    private Set<Class<? extends Table<?>>> referenceTables;

    private final Set<Class<? extends Table<?>>> tableTypesWithInserts = new HashSet<>();

    /**
     * Constructs DB cleanup rule.
     *
     * @param schema schema to clean data in
     * @param dsl database access
     */
    DbCleanupRule(Schema schema, DSLContext dsl) {
        this.schema = schema;
        this.dsl = dsl;
        this.schemaCleaner = new SchemaCleaner(schema, dsl);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try (SchemaCleaner.Monitor monitor = schemaCleaner.monitor(description)) {
                    base.evaluate();
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
            }
            if (methodLevel != null) {
                truncateTypes.addAll(Arrays.asList(methodLevel.truncate()));
                retainTypes.addAll(Arrays.asList(methodLevel.retain()));
            }
        }

        public Set<Class<? extends Table<?>>> getTruncateTypes() {
            return truncateTypes;
        }

        public Set<Class<? extends Table<?>>> getRetainTypes() {
            return retainTypes;
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
    }
}
