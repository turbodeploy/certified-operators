package com.vmturbo.history.listeners;

import org.immutables.value.Value;

/**
 * Structure to hold configuration values that need to be passed when creating an new
 * {@link TopologyCoordinator} instance. This eliminates the possibility of supplying the
 * values in a wrong order that's compatible with the constructor signature.
 */
@Value.Immutable
public interface TopologyCoordinatorConfig {
    /**
     * How much of the recent processing status to maintain.
     *
     * @return window size in seconds
     */
    int topologyRetentionSecs();

    /**
     * How long to wait for a received topology to be processed before skipping it.
     *
     * @return time limit in seconds
     */
    int ingestionTimeoutSecs();

    /**
     * How long to wait for hourly rollups to be initiated before forcing the associated
     * ingestions to resolved states.
     *
     * @return time limit in seconds
     */
    int hourlyRollupTimeoutSecs();

    /**
     * Maximum time to allow without running repartitioning.
     *
     * @return time limit in seconds
     */
    int repartitioningTimeoutSecs();

    /**
     * Maximum time for the processing loop to sleep before checking for more work.
     *
     * @return max sleep time in seconds
     */
    int processingLoopMaxSleepSecs();

    /**
     * Topology context id reserved for realtime topology.
     *
     * @return realtime topology context id
     */
    long realtimeTopologyContextId();
}
