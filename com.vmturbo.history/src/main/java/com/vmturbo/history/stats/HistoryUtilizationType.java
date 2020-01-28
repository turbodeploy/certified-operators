package com.vmturbo.history.stats;

import javax.annotation.Nullable;

/**
 * History Utilization Type.
 * This enum is persisted, do not change existing entries, only add to the end.
 */
public enum HistoryUtilizationType {
    /**
     * Percentile.
     */
    Percentile,
    /**
     * Timeslot.
     */
    Timeslot;

    /**
     * Finds appropriate {@link HistoryUtilizationType} value by its identifier.
     *
     * @param number which should uniquely identify {@link HistoryUtilizationType}
     * value.
     * @return {@link HistoryUtilizationType} value corresponding to the specified
     * identifier.
     */
    @Nullable
    public static HistoryUtilizationType forNumber(int number) {
        for (HistoryUtilizationType value : values()) {
            if (value.ordinal() == number) {
                return value;
            }
        }
        return null;
    }
}
