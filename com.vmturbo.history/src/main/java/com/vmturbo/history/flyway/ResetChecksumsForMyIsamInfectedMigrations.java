package com.vmturbo.history.flyway;

import java.sql.Connection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.BaseFlywayCallback;
import org.flywaydb.core.api.callback.FlywayCallback;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * This class executes three Flyway callbacks to reset checskums for migrations that were changed
 * in order to fix bug OM-55595.
 */
public class ResetChecksumsForMyIsamInfectedMigrations extends BaseFlywayCallback {

    private final List<FlywayCallback> callbacks;

    /**
     * Create a new instance of the callback, to be registered with Flyway prior to starting the
     * migration operation in which it is to operate.
     */
    public ResetChecksumsForMyIsamInfectedMigrations() {
        // create individual callbacks for the three migrations we need to reset
        this.callbacks = ImmutableList.of(
                new ResetMigrationChecksumCallback("1.0", ImmutableSet.of(-738285143), 1400110072),
                new ResetMigrationChecksumCallback("1.10", ImmutableSet.of(-1977103609), 396330773),
                new ResetMigrationChecksumCallback("1.16", ImmutableSet.of(-1170969968), 58983227)
        );
    }

    @Override
    public void beforeValidate(final Connection connection) {
        // execute all three of our callbacks
        callbacks.forEach(callback -> callback.beforeValidate(connection));
    }
}
