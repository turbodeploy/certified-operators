package com.vmturbo.action.orchestrator.stats.rollup;

/**
 * Interface to provide multiple rollup algorithms.
 */
public interface IActionStatRollupScheduler {

    /**
     * Schedule rollups. The rollups will run asynchronously.
     */
    void scheduleRollups();

}
