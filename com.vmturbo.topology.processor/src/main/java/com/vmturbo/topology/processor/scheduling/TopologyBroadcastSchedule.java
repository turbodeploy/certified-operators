package com.vmturbo.topology.processor.scheduling;

import java.util.concurrent.ScheduledFuture;

import javax.annotation.Nonnull;

/**
 * The broadcast schedule for the topology to other components.
 * Used to schedule topology broadcasts at fixed intervals.
 */
public class TopologyBroadcastSchedule extends Schedule {
    /**
     * Create a new TopologyBroadcastSchedule.
     *
     * @param scheduledTask The task that actually executes the scheduled topology broadcast
     *                      at fixed intervals.
     * @param broadcastIntervalMillis The interval at which to broadcast the topology.
     */
    TopologyBroadcastSchedule(@Nonnull ScheduledFuture<?> scheduledTask,
                              long broadcastIntervalMillis) {
        super(scheduledTask, broadcastIntervalMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "TopologyBroadcastSchedule to broadcast every " + scheduleIntervalMillis + " ms";
    }
}
