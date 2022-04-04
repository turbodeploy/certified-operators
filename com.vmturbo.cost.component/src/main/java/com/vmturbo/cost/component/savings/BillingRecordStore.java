package com.vmturbo.cost.component.savings;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * Interface to fetch billing records from billed cost store.
 */
public interface BillingRecordStore {
    /**
     * Get billing records that have been marked as changed within the given start and end time,
     * for the given subset of entityIds.
     *
     * @param lastUpdatedStartTime Previous max value of last_updated when we queried during last
     *      processing. During the current processing, want to get all changes that happened since
     *      that previous time.
     * @param lastUpdatedEndTime End time to use for last_updated as we don't want to risk getting
     *      records that might be getting updated currently (in case billing update is happening
     *      while we are querying, we would get partial incorrect results otherwise). So get only
     *      records that have been updated at least 10 minutes back.
     * @param entityIds List of entity ids to get records for.
     * @return Stream of BillingChangeRecord.
     */
    Stream<BillingChangeRecord> getBillingChangeRecords(long lastUpdatedStartTime,
            long lastUpdatedEndTime, @Nonnull List<Long> entityIds);
}
