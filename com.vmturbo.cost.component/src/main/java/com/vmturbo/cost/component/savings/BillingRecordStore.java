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
     * @param endTime get records with timestamps before the end time.
     * @param entityIds List of entity ids to get records for.
     * @return Stream of bill records.
     */
    Stream<BillingRecord> getUpdatedBillRecords(long lastUpdatedStartTime,
            LocalDateTime endTime, @Nonnull Set<Long> entityIds, int savingsDaysToSkip);

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
