package com.vmturbo.action.orchestrator.stats.rollup.v2;

/**
 * There are two versions of the action stat rollup algorithm.
 */
public enum RollupAlgorithmVersion {
    /**
     * V1 is the classic version, where daily stats are rolled up on the next day and monthly
     * stats are rolled up on the next month.
     */
    V1,

    /**
     * V2 is the next version, where an hour of data is rolled into the hourly, daily, and monthly
     * tables at the same time.
     */
    V2
}
