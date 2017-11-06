package com.vmturbo.plan.orchestrator.scheduled;

import java.util.concurrent.ScheduledFuture;

import javax.annotation.Nonnull;

/**
 * The plan project schedule for recurring plan.
 *
 */
public class PlanProjectSchedule extends PlanSchedule {
    private final long planProjectId;

    /**
     * Create a new plan project PlanSchedule.
     *
     * @param scheduledTask The task that actually executes the recurring plan
     * @param planProjectId The ID of the plan project.
     */
    PlanProjectSchedule(@Nonnull ScheduledFuture<?> scheduledTask, long planProjectId) {
        super(scheduledTask);
        this.planProjectId = planProjectId;

    }

    /**
     * The ID of the plan project to which this schedule applies.
     *
     * @return The ID of the plan project to which this schedule applies.
     */
    public long getPlanProjectId() {
        return planProjectId;
    }
}
