package com.vmturbo.testbases.flyway;

import junit.framework.AssertionFailedError;

/**
 * Exceptions created to represent violations in Flyway migration integrity checks.
 *
 * <p>The top-level type is abstract; subclasses should be used for violations.</p>
 */
public abstract class FlywayMigrationIntegrityException extends AssertionFailedError {

    private String advice = null;

    /**
     * Create a new instance.
     *
     * @param s violation message
     */
    public FlywayMigrationIntegrityException(final String s) {
        super(s);
    }

    /**
     * Advice is added to exceptions that for violations that are not white-listed.
     *
     * <p>The advice always starts out empty and so must be added after exception creation
     * if needed.</p>
     *
     * @param advice advice to a be added to this exception
     */
    void setAdvice(String advice) {
        this.advice = advice;
    }

    /**
     * We override to includ advice if present.
     *
     * @return original message plus advice, if any was added
     */
    @Override
    public String getMessage() {
        return super.getMessage() + (advice != null ? "\n" + advice : "");
    }

    /**
     * Exception representing a migration change violation, meaning that a non-whitelisted
     * change to a preexisting migration has been detected.
     */
    public static class FlywayChangedMigrationException extends FlywayMigrationIntegrityException {
        /**
         * Create a new instance.
         *
         * @param s violation message
         */
        public FlywayChangedMigrationException(final String s) {
            super(s);
        }
    }

    /**
     * Exception representing a migration collision violation, meaning that either the working
     * tree or the base migration (or both) contain multiple migration files for the same migration
     * version string.
     */
    public static class FlywayCollidingMigrationsException extends FlywayMigrationIntegrityException {
        /**
         * Create a new instance.
         *
         * @param s violation message
         */
        public FlywayCollidingMigrationsException(final String s) {
            super(s);
        }
    }

    /**
     * Exception representing a migration insertion violation, meaning that a migration file appears
     * with a non-whitelisted version, and that version precedes the maximum migration version
     * available in the base commit.
     */
    public static class FlywayInsertedMigrationException extends FlywayMigrationIntegrityException {
        /**
         * Create a new instance.
         *
         * @param s violation message
         */
        public FlywayInsertedMigrationException(final String s) {
            super(s);
        }
    }

    /**
     * Exception representing a migration deletion violation, meaning that there was a migration
     * for a non-whitelisted migration version in the base commit, but there is no migration for
     * that version in the working tree.
     */
    public static class FlywayDeletedMigrationException extends FlywayMigrationIntegrityException {
        /**
         * Create a new instance.
         *
         * @param s violation message
         */
        public FlywayDeletedMigrationException(final String s) {
            super(s);
        }
    }
}
