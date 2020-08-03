package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import javax.annotation.Nonnull;


/**
 * Historical Demand Data Type Used in RI Buy Analysis.
 */
public enum ReservedInstanceHistoricalDemandDataType {
    /**
     * Implies that Allocation based data to be used for RI Buy Analysis.
     */
    ALLOCATION("count_from_source_topology"),

    /**
     * Implies that Consumption based data to be used for RI Buy Analysis.
     */
    CONSUMPTION("count_from_projected_topology");

    /**
     * Stores field name from 'instance_type_hourly_by_week' DB table that contains corresponding
     * weighted values.
     */
    private final String dbFieldName;

    ReservedInstanceHistoricalDemandDataType(@Nonnull String fieldName) {
        this.dbFieldName = fieldName;
    }

    /**
     * Returns field name.
     *
     * @return field name.
     */
    @Nonnull
    public String getDbFieldName() {
        return dbFieldName;
    }
}