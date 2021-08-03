/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.components.common;

import javax.annotation.Nonnull;

/**
 * {@link HistoryUtilizationType} describes all supported history aggregations. {@link
 * Enum#ordinal()} is used to store data in DB, i.e. value declaration order is important.
 */
public enum HistoryUtilizationType {
    /**
     * Marks values aggregated using percentile function.
     */
    Percentile("percentile"),
    /**
     * Marks values aggregated using time slot function.
     */
    Timeslot("timeslot"),
    /**
     * Marks values aggregated using historical smoothing.
     */
    Smoothed("smoothed");

    private final String apiParameterName;

    HistoryUtilizationType(@Nonnull String apiParameterName) {
        this.apiParameterName = apiParameterName;
    }

    /**
     * Finds appropriate {@link HistoryUtilizationType} value by its identifier.
     *
     * @param number which should uniquely identify {@link HistoryUtilizationType}
     *                 value.
     * @return {@link HistoryUtilizationType} value corresponding to the specified
     *                 identifier.
     * @throws IllegalArgumentException in case number does not correspond to any of the
     *                 {@link HistoryUtilizationType} value ordinals.
     */
    @Nonnull
    public static HistoryUtilizationType forNumber(int number) throws IllegalArgumentException {
        for (HistoryUtilizationType value : values()) {
            if (value.ordinal() == number) {
                return value;
            }
        }
        throw new IllegalArgumentException("Invalid HistoryUtilizationType number " + number);
    }

    /**
     * Returns API parameter name matching current value.
     *
     * @return API parameter name matching current value.
     */
    @Nonnull
    public String getApiParameterName() {
        return apiParameterName;
    }
}
