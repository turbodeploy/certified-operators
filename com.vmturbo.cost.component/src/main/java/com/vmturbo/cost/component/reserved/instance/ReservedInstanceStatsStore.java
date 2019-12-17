package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.Result;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceStatsFilter;

/**
 * Type to generically handle operations for RI statistic stores.
 *
 * @param <T> type of ReservedInstanceStatsFilter that is used for querying the
 *                            RI store.
 */
abstract class ReservedInstanceStatsStore<T extends ReservedInstanceStatsFilter> {

    /**
     * Returns the collection of latest statistics from the RI stats store.
     *
     * @param filter for querying the RI stat store.
     * @return collection of latest statistics.
     */
    Collection<ReservedInstanceStatsRecord> getLatestReservedInstanceStatsRecords(
            @Nonnull final T filter) {
        final Result<?> records = getLatestRecords(filter);
        return records.stream()
                .map(ReservedInstanceUtil::convertRIUtilizationCoverageRecordToRIStatsRecord)
                .collect(Collectors.toList());
    }

    /**
     * Queries the specific RI stat table in the db for latest records and returns the result.
     *
     * @param filter for querying the table in the db.
     * @return Result of the query.
     */
    abstract Result getLatestRecords(@Nonnull T filter);
}
