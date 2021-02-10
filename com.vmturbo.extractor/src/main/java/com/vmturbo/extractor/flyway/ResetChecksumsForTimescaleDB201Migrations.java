package com.vmturbo.extractor.flyway;

import java.sql.Connection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.BaseFlywayCallback;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.flywaydb.core.api.migration.MigrationChecksumProvider;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * This call back removes checksums (i.e. sets them to null) for the V1.0, V1.1, V1.4 V1.6.2 and V1.13 migrations.
 *
 * <p>The migration needed to support upgrading timescaledb to V2.0.1.</p>
 *
 * <p>The new implementation does not implement {@link MigrationChecksumProvider}, and so the
 * correct checksum value is now null.</p>
 */
public class ResetChecksumsForTimescaleDB201Migrations extends BaseFlywayCallback {

    private final List<FlywayCallback> callbacks;

    /**
     * Create a new instance of the callback, to be registered with Flyway prior to starting the
     * migration operation in which it is to operate.
     */
    public ResetChecksumsForTimescaleDB201Migrations() {
        // create individual callbacks for the three migrations we need to reset
        this.callbacks = ImmutableList.of(
                new ResetMigrationChecksumCallback("1.0", ImmutableSet.of(45976503), -1192587644),
                new ResetMigrationChecksumCallback("1.1", ImmutableSet.of(212504811), -1258796301),
                new ResetMigrationChecksumCallback("1.4", ImmutableSet.of(2091129349), 420804163),
                new ResetMigrationChecksumCallback("1.6.2", ImmutableSet.of(-87596652), -954894531),
                new ResetMigrationChecksumCallback("1.13", ImmutableSet.of(1317853397), 119327364)

        );
    }

    @Override
    public void beforeValidate(final Connection connection) {
        // execute all three of our callbacks
        callbacks.forEach(callback -> callback.beforeValidate(connection));
    }
}
