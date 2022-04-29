package com.vmturbo.topology.processor.identity.recurrenttasks;

import static com.vmturbo.topology.processor.db.tables.AssignedIdentity.ASSIGNED_IDENTITY;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

import org.jooq.DSLContext;

import com.vmturbo.topology.processor.identity.StaleOidManagerImpl;

/**
 * Implementation of a {@link RecurrentTask} for updating the last seen on the existing records
 * in the entity store.
 */
public class OidTimestampUpdateTask extends RecurrentTask {

    private final Supplier<Set<Long>> getCurrentOids;
    private static final String UPDATE_TIMESTAMPS = "update_timestamps";
    private final StaleOidManagerImpl.OidExpirationResultRecord oidExpirationResultRecord;

    /**
     * Gets an instance of a {@link OidTimestampUpdateTask}.
     * @param taskStartingTime the starting time of the task
     * @param clock the clock to use
     * @param context the database dsl context
     * @param getCurrentOids supplier of existing oids in the system
     * @param oidExpirationResultRecord record to store in the recurrent operations table
     */
    public OidTimestampUpdateTask(final long taskStartingTime, @Nonnull final Clock clock, @Nonnull final DSLContext context,
                                  @Nonnull final Supplier<Set<Long>> getCurrentOids,
                                  @Nonnull final StaleOidManagerImpl.OidExpirationResultRecord oidExpirationResultRecord) {
        super(taskStartingTime, clock, context);
        this.getCurrentOids = getCurrentOids;
        this.oidExpirationResultRecord = oidExpirationResultRecord;
    }

    @Override
    public int performTask() {
        final Set<Long> currentOids = this.getCurrentOids.get();
        int updatedRecords = 0;
        final Stopwatch stopwatch = Stopwatch.createStarted();
        final Timestamp currentTimeStamp = Timestamp.from(Instant.ofEpochMilli(currentTimeMs));
        for (List<Long> batchedOids : Iterables.partition(currentOids, BATCH_SIZE)) {
            updatedRecords += context.update(ASSIGNED_IDENTITY)
                    .set(ASSIGNED_IDENTITY.LAST_SEEN, currentTimeStamp)
                    .where(ASSIGNED_IDENTITY.ID.in(batchedOids)).execute();
        }
        oidExpirationResultRecord.setUpdatedRecords(updatedRecords);
        OID_EXPIRATION_EXECUTION_TIME.labels(UPDATE_TIMESTAMPS).observe((double)stopwatch.elapsed(TimeUnit.SECONDS));
        return updatedRecords;
    }

    @Override
    public RecurrentTasksEnum getRecurrentTaskType() {
        return RecurrentTasksEnum.OID_TIMESTAMP_UPDATE;
    }
}
