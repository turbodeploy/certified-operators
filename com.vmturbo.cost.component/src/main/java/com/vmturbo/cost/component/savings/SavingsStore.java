package com.vmturbo.cost.component.savings;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.savings.calculator.SavingsValues;

/**
 * Interface to write stats to store.
 */
public interface SavingsStore {
    /**
     * Saves states for the given daily period to the store.
     *
     * @param values Stats values to write.
     * @return Set of timestamps of the daily records written, used for rollup later.
     * @throws EntitySavingsException Thrown on DB error.
     */
    Set<Long> writeDailyStats(@Nonnull List<SavingsValues> values) throws EntitySavingsException;

    /**
     * Delete all stats from the hourly, daily, and monthly tables.
     *
     * @param uuids list of UUIDs for which to delete stats.
     */
    void deleteStats(@Nonnull Set<Long> uuids);

    /**
     * Given a set of entity OIDs, determine which of the OIDs in the set do not have a scope record
     * in the entity_cloud_scope table.
     *
     * @param entityOids a set of entity OIDs
     * @return entity OIDs that don't have scope records
     */
    Set<Long> getEntitiesWithoutScopeRecords(Set<Long> entityOids);
}
