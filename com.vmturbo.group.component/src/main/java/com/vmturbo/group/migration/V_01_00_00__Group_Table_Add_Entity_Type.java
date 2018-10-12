package com.vmturbo.group.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.Grouping;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.group.group.GroupStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This migration fills the "entity_type" column in the group table from the Group blob
 * in the "group" table.
 *
 * This is associated with the "V1_7__drop_unique_group_name.sql"
 */
public class V_01_00_00__Group_Table_Add_Entity_Type implements Migration {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Group migrations are done in a batch.
     * We specify it statically instead of via dynamic config because this migration is
     * something that only runs once, so it's not worth the overhead.
     */
    private static final int BATCH_SIZE = 100;

    private final Object migrationInfoLock = new Object();

    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo =
            MigrationProgressInfo.newBuilder();

    private final DSLContext dslContext;

    public V_01_00_00__Group_Table_Add_Entity_Type(@Nonnull final DSLContext dslContext) {
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

        dslContext.transaction(config -> {
            final DSLContext transactionContext = DSL.using(config);
            final Result<GroupingRecord> groupsToMigrate =
                transactionContext.selectFrom(Tables.GROUPING)
                    .where(Tables.GROUPING.ENTITY_TYPE.eq(-1))
                    .fetch();

            logger.info("Found {} groups to migrate.", groupsToMigrate.size());

            final List<GroupingRecord> groupsToUpdate = new ArrayList<>();
            final List<GroupingRecord> groupsToDelete = new ArrayList<>();
            groupsToMigrate.forEach(groupingRecord -> {
                final Group.Type groupType = Group.Type.forNumber(groupingRecord.getType());
                if (groupType != null) {
                    try {
                        final int entityType = getGroupBlobEntityType(groupType, groupingRecord.getGroupData());
                        groupingRecord.setEntityType(entityType);
                        groupsToUpdate.add(groupingRecord);
                    } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                        // This group is invalid, and wouldn't return anything if the user attempts
                        // to query it anyway. Just delete it.
                        logger.error("Group {} (id: {}) does not contain a valid group data blob. Deleting.",
                                groupingRecord.getName(), groupingRecord.getId());
                        groupsToDelete.add(groupingRecord);
                    }
                } else {
                    // This group is invalid, and wouldn't return anything if the user attempts
                    // to query it anyway. Just delete it.
                    logger.error("Group {} (id: {}) does not contain a valid group type. Deleting.");
                    groupsToDelete.add(groupingRecord);
                }
            });

            logger.info("Updating {} groups.", groupsToUpdate.size());

            Lists.partition(groupsToUpdate, BATCH_SIZE).forEach(updateBatch -> {
                logger.info("Batch update of size {}", updateBatch.size());
                final int[] results = transactionContext.batchUpdate(updateBatch).execute();
                int errorCount = 0;
                for (final int rowsUpdated : results) {
                    if (rowsUpdated != 1) {
                        errorCount++;
                    }
                }
                if (errorCount > 0) {
                    // This should never happen because we got the records to update in the
                    // same transaction, and we know they need to be updated.
                    throw new IllegalStateException(errorCount + "(out of " + updateBatch.size() +
                        ") row updates didn't actually update any rows.");
                }
            });

            logger.info("Deleting {} groups.", groupsToDelete.size());

            Lists.partition(groupsToDelete, BATCH_SIZE).forEach(deleteBatch -> {
                logger.info("Batch delete of size {}", deleteBatch.size());
                final int[] results = transactionContext.batchDelete(deleteBatch).execute();
                int errorCount = 0;
                for (final int rowsUpdated : results) {
                    if (rowsUpdated != 1) {
                        errorCount++;
                    }
                }
                if (errorCount > 0) {
                    // This should never happen because we got the records to update in the
                    // same transaction, and we know they need to be deleted.
                    throw new IllegalStateException(errorCount + "(out of " + deleteBatch.size() +
                            ") row deletions didn't actually delete any rows.");
                }
            });
        });

        logger.info("Finished migration!");

        synchronized (migrationInfoLock) {
            return migrationInfo.setStatus(MigrationStatus.SUCCEEDED)
                .setCompletionPercentage(100)
                .setStatusMessage("Finished refreshing groups - all groups should now have entity type set.")
                .build();
        }
    }

    private int getGroupBlobEntityType(final Group.Type groupType, final byte[] blob)
            throws InvalidProtocolBufferException {
        switch (groupType) {
            case GROUP:
                return GroupInfo.parseFrom(blob).getEntityType();
            case CLUSTER:
                final ClusterInfo cluster = ClusterInfo.parseFrom(blob);
                return cluster.getClusterType() == Type.COMPUTE ?
                    EntityType.PHYSICAL_MACHINE_VALUE : EntityType.STORAGE_VALUE;
            case TEMP_GROUP:
                return TempGroupInfo.parseFrom(blob).getEntityType();
            default:
                throw new IllegalArgumentException("Invalid/unhandled group type: " + groupType);
        }
    }
}
