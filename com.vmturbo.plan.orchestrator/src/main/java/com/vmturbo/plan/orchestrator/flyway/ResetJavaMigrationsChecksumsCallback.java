package com.vmturbo.plan.orchestrator.flyway;

import java.sql.Connection;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.BaseFlywayCallback;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * This callback resets the callback of Java migrations to null. The reason behind this is that
 * Java migrations are subject to change, so making sure the checksum has not changed is not
 * a maintainable solution.
 *
 * <p>In this particular case, the Java migrations were moved to a different
 * package and a constructor was added. This caused the checksum to change and Flyway migrations
 * would fail without this callback.</p>
 */
public class ResetJavaMigrationsChecksumsCallback extends BaseFlywayCallback {

    @Override
    public void beforeValidate(final Connection connection) {
        new ResetMigrationChecksumCallback("2.13", ImmutableSet.of(1843390576),
                null).beforeValidate(connection);
        new ResetMigrationChecksumCallback("2.16", ImmutableSet.of(936988417),
                null).beforeValidate(connection);
    }
}
