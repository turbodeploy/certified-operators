package com.vmturbo.cost.component.savings;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.savings.calculator.SavingsValues;

/**
 * Interface to write stats to store.
 */
public interface StatsWriter {
    /**
     * Saves states for the given daily period to the store.
     *
     * @param values Stats values to write.
     * @return Set of timestamps of the daily records written, used for rollup later.
     * @throws EntitySavingsException Thrown on DB error.
     */
    Set<Long> writeDailyStats(@Nonnull List<SavingsValues> values) throws EntitySavingsException;
}
