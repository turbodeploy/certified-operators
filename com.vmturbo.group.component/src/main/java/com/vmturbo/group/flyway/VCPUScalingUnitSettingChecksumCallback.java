package com.vmturbo.group.flyway;

import java.sql.Connection;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.BaseFlywayCallback;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * This callback sets the callback of vcpu scaling migrations to new value.
 * We had to modify the 1.68 migration script to fix one known issue, hence the checksum in db for 1.68
 * migration also need to be updated, to pass the checksum validation in flyway.
 */
public class VCPUScalingUnitSettingChecksumCallback extends BaseFlywayCallback {

    @Override
    public void beforeValidate(final Connection connection) {
        new ResetMigrationChecksumCallback("1.68", ImmutableSet.of(714350328), 1817126329).beforeValidate(connection);
    }
}
