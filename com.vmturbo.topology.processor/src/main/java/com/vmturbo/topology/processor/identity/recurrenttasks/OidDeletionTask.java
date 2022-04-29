package com.vmturbo.topology.processor.identity.recurrenttasks;

import static com.vmturbo.topology.processor.db.tables.AssignedIdentity.ASSIGNED_IDENTITY;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

/**
 * Implementation of a {@link RecurrentTask} that deletes expired records from the database.
 */
public class OidDeletionTask extends RecurrentTask {

    final boolean shouldDeleteExpiredRecords;
    final int expiredRecordsRetentionDays;

    /**
     * Builds an instance of a {@link OidDeletionTask}.
     * @param taskStartingTime starting time of the task
     * @param clock shared clock with other tasks
     * @param context for database access
     * @param shouldDeleteExpiredRecords whether records should be expired
     * @param expiredRecordsRetentionDays the amount of days before deleting an expired record
     */
    public OidDeletionTask(long taskStartingTime, final @Nonnull Clock clock, DSLContext context, final boolean shouldDeleteExpiredRecords,
                           int expiredRecordsRetentionDays) {
        super(taskStartingTime, clock, context);
        this.shouldDeleteExpiredRecords = shouldDeleteExpiredRecords;
        this.expiredRecordsRetentionDays = expiredRecordsRetentionDays;
    }

    @Override
    public int performTask() {
        int totalDeletedRecords = 0;
        int deletedRecordsPerBatch;
        if (shouldDeleteExpiredRecords) {
            do {
                Timestamp retentionDate = Timestamp.from(Instant.ofEpochMilli(currentTimeMs - TimeUnit.DAYS.toMillis(expiredRecordsRetentionDays)));
                deletedRecordsPerBatch = context.deleteFrom(ASSIGNED_IDENTITY)
                        .where(ASSIGNED_IDENTITY.LAST_SEEN.lessThan(retentionDate))
                        .and(ASSIGNED_IDENTITY.EXPIRED.isTrue())
                        .limit(BATCH_SIZE)
                        .execute();
                totalDeletedRecords += deletedRecordsPerBatch;
            } while (deletedRecordsPerBatch > 0);
        }
        return totalDeletedRecords;
    }

    @Override
    public RecurrentTasksEnum getRecurrentTaskType() {
        return RecurrentTasksEnum.OID_DELETION;
    }

}