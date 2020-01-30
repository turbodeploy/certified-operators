/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.history.stats;

import javax.annotation.Nonnull;

import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;

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
    Timeslot("timeslot");
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
     * @throws VmtDbException in case number does not correspond to any of the
     *                 {@link HistoryUtilizationType} value ordinals.
     */
    @Nonnull
    public static HistoryUtilizationType forNumber(int number) throws VmtDbException {
        for (HistoryUtilizationType value : values()) {
            if (value.ordinal() == number) {
                return value;
            }
        }
        throw new VmtDbException(VmtDbException.READ_ERR,
                        HistUtilization.HIST_UTILIZATION.getName());
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
