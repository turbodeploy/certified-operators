package com.vmturbo.history.flyway;

import java.util.Collections;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * Class containing callbacks required by changes to migrations committed in releases 8.5.2 and then
 * again in 8.5.4. (The migrations are not in 8.5.3 because the MR commit that introduced the
 * migrations was reverted in 8.5.3.).
 *
 * <p>We had separate implementations of the migration for MariaDB (V1.66) and Postgres (V1.4). The
 * migrations needed to be changed because the originals were not idempotent, and they performed
 * potentially long-running operations where idempotency is particularly important.</p>
 *
 * <p>The outer class is just a container for two callback classes - one for MariaDB and one for
 * Postgres.</p>
 */
public class MigrationCallbacksForV1v66AndV1v4 {
    /**
     * Callback implementation for postgres.
     */
    public static class ForPostgresV1v4 extends ResetMigrationChecksumCallback {

        /**
         * Create a new instance.
         */
        public ForPostgresV1v4() {
            super("1.4", Collections.singleton(379472195), -676761494);
        }
    }

    /**
     * Callback implementation for MariaDB.
     */
    public static class ForMariaDBV1v66 extends ResetMigrationChecksumCallback {
        /**
         * Create a new instance.
         */
        public ForMariaDBV1v66() {
            super("1.66", ImmutableSet.of(-1677439971, -1499992363), -1579115327);
        }
    }
}