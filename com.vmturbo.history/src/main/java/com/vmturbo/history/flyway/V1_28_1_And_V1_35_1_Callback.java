package com.vmturbo.history.flyway;

import java.sql.Connection;

import com.google.common.collect.ImmutableSet;

import org.flywaydb.core.api.callback.BaseFlywayCallback;
import org.flywaydb.core.api.migration.MigrationChecksumProvider;

import com.vmturbo.sql.utils.flyway.ResetMigrationChecksumCallback;

/**
 * This call back removes checksums (i.e. sets them to null) for the V1.28.1 and V1.35.1 migrations.
 *
 * <p>These were identical migrations that populate the avaialble_timestamps table when it's
 * created. The V1.35.1 copy was created because V1.28.1 fell into a gap when 7.17 and 7.21
 * branches were unified, and will not be executed by installations that did not upgrade from
 * a 7.17 branch.</p>
 *
 * <p>The migration needed to be fixed to deal with installations that use a database with a non-
 * UTC timestamp, since in those cases it was capable of attempting to insert timestamps that
 * would be interpreted, in such time zones, as invalid because they fall within the one-hour
 * gap that happens at the beginning of daylight savings time.</p>
 *
 * <p>The new implementation does not implement {@link MigrationChecksumProvider}, and so the
 * correct checksum value is now null.</p>
 */
public class V1_28_1_And_V1_35_1_Callback extends BaseFlywayCallback {

    @Override
    public void beforeValidate(final Connection connection) {
        new ResetMigrationChecksumCallback("1.28.1", ImmutableSet.of(564074413), null)
                .beforeValidate(connection);
        new ResetMigrationChecksumCallback("1.35.1", ImmutableSet.of(275378770), null)
                .beforeValidate(connection);
    }
}
