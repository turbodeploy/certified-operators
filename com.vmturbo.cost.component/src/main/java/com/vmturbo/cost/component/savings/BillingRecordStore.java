package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * Interface to fetch billing records from billed cost store.
 */
public interface BillingRecordStore {
    /**
     * Get billing records that have been marked as changed within the given start and end time,
     * for the given subset of entityIds. If an entity has a changed record for a day, all records
     * for that entity from that day will be included in the result.
     *
     * @param lastUpdatedStartTime Previous max value of last_updated when we queried during last
     *      processing. During the current processing, want to get all changes that happened since
     *      that previous time.
     * @param lastUpdatedEndTime End time to use for last_updated as we don't want to risk getting
     *      records that might be getting updated currently (in case billing update is happening
     *      while we are querying, we would get partial incorrect results otherwise). So get only
     *      records that have been updated at least 10 minutes back.
     * @param entityIds List of entity ids to get records for.
     * @return Stream of bill records.
     */
    Stream<BillingRecord> getUpdatedBillRecords(long lastUpdatedStartTime,
            long lastUpdatedEndTime, @Nonnull Set<Long> entityIds);

    /**
     * Get all bill records for a specified set of entities within a specified period.
     * This method is needed by the scenario generation use case.
     *
     * @param startTime period start time
     * @param endTime period end time
     * @param entityIds entities IDs
     * @return Stream of bill records
     */
    Stream<BillingRecord> getBillRecords(LocalDateTime startTime, LocalDateTime endTime,
            @Nonnull Set<Long> entityIds);
}
