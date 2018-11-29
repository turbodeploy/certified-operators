package com.vmturbo.group.migration;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.db.Tables;

/**
 * This migration deletes the discovered groups and policies. Previous to this
 * version, we were identifying groups by displayName. But different groups can
 * have the same displayNames(duplicates). To uniquely identify a group, we have
 * to use either {GroupName,EntityType} or {ConstraintName, EntityType} combination.
 * With the new scheme, the existing discovered groups and polices would no
 * longer be relevant and hence they have to be deleted from the DB.
 */
public class V_01_00_01__Drop_Discovered_Groups_Policies implements Migration {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final Object migrationInfoLock = new Object();

    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo =
            MigrationProgressInfo.newBuilder();


    public V_01_00_01__Drop_Discovered_Groups_Policies(@Nonnull final DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    @Override
    public MigrationStatus getMigrationStatus() {
        synchronized (migrationInfoLock) {
            return migrationInfo.getStatus();
        }
    }

    @Override
    public MigrationProgressInfo getMigrationInfo() {
        synchronized (migrationInfoLock) {
            return migrationInfo.build();
        }
    }

    @Override
    public MigrationProgressInfo startMigration() {
        logger.info("Starting migration...");
        synchronized (migrationInfoLock) {
            migrationInfo.setStatus(MigrationStatus.RUNNING);
        }

        // If the Discovered_By_Id column is not null, it means
        // A group is
        dslContext.transaction(config -> {
            final DSLContext transactionContext = DSL.using(config);
            transactionContext.deleteFrom(Tables.GROUPING)
                    .where(Tables.GROUPING.DISCOVERED_BY_ID.isNotNull())
                    .execute();
            transactionContext.deleteFrom(Tables.POLICY)
                    .where(Tables.POLICY.DISCOVERED_BY_ID.isNotNull())
                    .execute();
        });

        // Delete all discovered groups and policies.
        logger.info("Finished migration!");

        synchronized (migrationInfoLock) {
            return migrationInfo.setStatus(MigrationStatus.SUCCEEDED)
                .setCompletionPercentage(100)
                .setStatusMessage("Removed all the old discovered groups and policies.")
                .build();
        }

    }
}
