package com.vmturbo.action.orchestrator.flyway;

import java.sql.Connection;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.BaseFlywayCallback;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * This callback updates the migration checksum for the V1.24 migration.
 *
 * <p>This migration adds a primary key on the `id` column of `workflow` table.</p>
 *
 * <p>This becomes a problem on systems that run on MariaDB 10.4 version, because V1.4.1 migration
 * has already added a unique index on the `id` column (which is not null) so MariaDB already uses
 * that column as a primary key. When the primary key is about to be created, an error is thrown.
 * In newer versions of MariaDB, this error is not thrown and the primary key index is created
 * successfully.</p>
 *
 * <p>As a solution, the unique index is dropped before adding the primary key. In order to bring
 * the database state consistent between instances that have already run the V1.24 migration
 * successfully and the instances that have not, another migration is introduced (V1.28) that drops
 * the unique index if it exists.</p>
 */
public class V1v24Callback extends BaseFlywayCallback {

    @Override
    public void beforeValidate(final Connection connection) {
        new ResetMigrationChecksumCallback("1.24", ImmutableSet.of(1546449123),
                1013957996).beforeValidate(connection);
    }
}
