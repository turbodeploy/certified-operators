package com.vmturbo.topology.processor.scheduling;

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A base class containing an event that can be scheduled to occur at fixed regular intervals.
 * Schedules are managed by a scheduler where they can be retrieved, set, cancelled, and reset.
 * Provides millisecond-level granularity for interacting with the schedule.
 */
public abstract class Schedule {
    protected final ScheduledFuture<?> scheduledTask;
    protected final long scheduleIntervalMillis;

    /**
     * Create a new Schedule.
     *
     * @param scheduledTask The task that actually executes the event associated with the schedule.
     * @param scheduleIntervalMillis The interval at which to discover the target in milliseconds.
     */
    Schedule(@Nonnull ScheduledFuture<?> scheduledTask, long scheduleIntervalMillis) {
        this.scheduledTask = Objects.requireNonNull(scheduledTask);
        this.scheduleIntervalMillis = scheduleIntervalMillis;
    }

    /**
     * Cancel the schedule. Halts future execution of the scheduled task.
     *
     * If a task is in the process of execution, it will NOT be interrupted.
     * This is because some tasks may issue a
     */
    public void cancel() {
        scheduledTask.cancel(false);
    }

    /**
     * Check if the schedule has been cancelled.
     *
     * @return True if the schedule has been cancelled, false otherwise.
     */
    public boolean isCancelled() {
        return scheduledTask.isCancelled();
    }

    /**
     * Get the time elapsed against the schedule in milliseconds.
     * This returns the difference in milliseconds between the discovery interval
     * and the remaining delay until the next scheduled discovery will be
     * requested for this target.
     *
     * @return The time elapsed against the schedule in milliseconds.
     */
    public long getElapsedTimeMillis() {
        return scheduleIntervalMillis - scheduledTask.getDelay(TimeUnit.MILLISECONDS);
    }

    /**
     * Get the discovery interval for the target in the requested units.
     * The schedule provides up to millisecond-level granularity.
     *
     * @param unit The unit in which to get the discovery interval.
     * @return The discovery interval in the requested unit.
     */
    public long getScheduleInterval(@Nonnull TimeUnit unit) {
        return unit.convert(scheduleIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the remaining delay until the next scheduled discovery for the target
     * in the requested unit.
     *
     * @param unit The unit in which to get the remaining delay.
     * @return The remaining delay until the next scheduled discovery in the
     *         requested unit.
     */
    public long getDelay(TimeUnit unit) {
        return scheduledTask.getDelay(unit);
    }

    /**
     * Get the data necessary to persist this schedule.
     *
     * @return The data necessary to persist the schedule.
     */
    public ScheduleData getScheduleData() {
        return new ScheduleData(scheduleIntervalMillis);
    }

    @Immutable
    public static class ScheduleData {
        private final long scheduleIntervalMillis;

        public ScheduleData() {
            scheduleIntervalMillis = -1;
        }

        public ScheduleData(long scheduleIntervalMillis) {
            this.scheduleIntervalMillis = scheduleIntervalMillis;
        }

        public long getScheduleIntervalMillis() {
            return scheduleIntervalMillis;
        }
    }
}