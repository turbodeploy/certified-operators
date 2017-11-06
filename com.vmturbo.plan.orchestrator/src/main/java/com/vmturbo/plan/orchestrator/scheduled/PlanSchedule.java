package com.vmturbo.plan.orchestrator.scheduled;

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A base class containing an event that can be scheduled to occur at fixed regular intervals.
 * PlanSchedule are managed by a PlanProjectScheduler where they can be retrieved, set and cancelled.
 */
public abstract class PlanSchedule {
    protected final ScheduledFuture<?> scheduledTask;

    /**
     * Create a new PlanSchedule.
     *
     * @param scheduledTask The task that actually executes the event associated with the schedule.
     */
    PlanSchedule(@Nonnull ScheduledFuture<?> scheduledTask) {
        this.scheduledTask = Objects.requireNonNull(scheduledTask);
    }

    /**
     * Cancel the schedule. Halts future execution of the scheduled task.
     *
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
     * Get the remaining delay until the next scheduled plan project
     * in the requested unit.
     *
     * @param unit The unit in which to get the remaining delay.
     * @return The remaining delay until the next scheduled plan project in the
     *         requested unit.
     */
    public long getDelay(TimeUnit unit) {
        return scheduledTask.getDelay(unit);
    }
}