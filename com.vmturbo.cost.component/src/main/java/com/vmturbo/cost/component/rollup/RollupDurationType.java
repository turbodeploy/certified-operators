package com.vmturbo.cost.component.rollup;

/**
 * Types of rollup done based on hourly stats table data.
 */
public enum RollupDurationType {
    /**
     * Default option, we write to hourly table directly.
     */
    HOURLY,

    /**
     * For rollup of hourly data to daily stats tables.
     */
    DAILY,

    /**
     * For rollup of daily data to monthly stats tables.
     */
    MONTHLY
}
