package com.vmturbo.cost.component.savings;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;

/**
 * This class implements the algorithm for calculating entity savings and investments.
 */
class SavingsCalculator {
    /**
     * Calculates savings and investments.
     *
     * @param events List of events
     * @param entityStateCache entity state cache
     * @param periodStartTime start time of the period
     * @param periodEndTime end time of the period
     */
    void calculate(@Nonnull final List<SavingsEvent> events, @Nonnull final EntityStateCache entityStateCache,
                   long periodStartTime, long periodEndTime) {

    }
}
