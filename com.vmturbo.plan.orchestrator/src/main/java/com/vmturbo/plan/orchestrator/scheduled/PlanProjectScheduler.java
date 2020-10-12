package com.vmturbo.plan.orchestrator.scheduled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectExecutor;
import com.vmturbo.plan.orchestrator.project.PlanProjectInfoNotFoundException;
import com.vmturbo.plan.orchestrator.project.PlanProjectNotFoundException;


/**
 * Maintains and runs scheduled recurring events such as nightly or user-defined plans.
 * Provided features:
 * 1. During component start up, it get all the plan projects from PlanProject table and schedule
 * them.
 * 2. It's intended to be used also for user-defined plan project.
 *
 * THREADING MODEL NOTES:
 * The scheduler is made thread-safe via synchronizing all publicly visible methods.
 * Clients should not synchronize on references they hold to a PlanProjectScheduler object.
 * The {@link PlanProjectScheduler} should make no external calls through its public interface
 * that potentially block on another call to the {@link PlanProjectScheduler} made from a
 * separate thread.
 */
@ThreadSafe
public class PlanProjectScheduler {
    private final Logger logger = LogManager.getLogger();
    private final ThreadPoolTaskScheduler schedulerExecutor;
    private final Map<Long, PlanProjectSchedule> planProjectTasks;
    private final PlanProjectDao planProjectDao;
    private final PlanProjectExecutor planProjectExecutor;

    /**
     * Create a {@link PlanProjectScheduler} for scheduling plan project.
     *
     * @param planProjectDao plan project dao
     * @param scheduleExecutor The executor to be used for scheduling tasks.
     * @param planProjectExecutor The executor for running plan projects
     */
    public PlanProjectScheduler(@Nonnull final PlanProjectDao planProjectDao,
                                @Nonnull final ThreadPoolTaskScheduler scheduleExecutor,
                                @Nonnull final PlanProjectExecutor planProjectExecutor) {
        this.schedulerExecutor = Objects.requireNonNull(scheduleExecutor);
        this.planProjectDao = planProjectDao;
        planProjectTasks = new HashMap<>();
        this.planProjectExecutor = planProjectExecutor;

        initializePlanProjectSchedule(scheduleExecutor);
    }

    /**
     * Set the plan project schedule, if the plan project is not already scheduled.
     *
     * return the existing schedule.
     *
     * @param planProjectId The ID of the plan project.
     * @return The plan project schedule for the input plan project id.
     */
    @Nonnull
    public synchronized PlanProjectSchedule setPlanProjectSchedule(final long planProjectId) throws
            PlanProjectNotFoundException, PlanProjectInfoNotFoundException {
        if (!planProjectTasks.containsKey(planProjectId)) {
            PlanProject project = planProjectDao.getPlanProject(planProjectId).orElseThrow(
                    () -> new PlanProjectNotFoundException(planProjectId));
            Trigger trigger = createCronTrigger(project.getPlanProjectInfo()).orElseThrow(
                    () -> new PlanProjectInfoNotFoundException(planProjectId));
            ScheduledFuture<?> scheduledTask = schedulerExecutor.schedule(
                    () -> executePlan(project), trigger);
            PlanProjectSchedule schedule = new PlanProjectSchedule(scheduledTask, planProjectId);
            planProjectTasks.put(planProjectId, schedule);
            return schedule;
        } else {
            return planProjectTasks.get(planProjectId);
        }
    }

    /**
     * Cancel the plan project schedule, if exists.
     * If not, the operation is skipped and return Optional.empty().
     *
     * @param planProjectId The ID of the plan project.
     * @return The plan project schedule for the input plan project id if it exists,
     * otherwise return Optional.empty().
     */
    @Nonnull
    public synchronized Optional<PlanProjectSchedule> cancelPlanProjectSchedule(final long planProjectId) {
        final Optional<PlanProjectSchedule> task = Optional.ofNullable(planProjectTasks.remove(planProjectId));
        task.ifPresent(PlanProjectSchedule::cancel);
        return task;
    }

    /**
     * Get the plan project schedule for a plan project.
     *
     * @param planProjectId The ID of the plan project whose schedule should be retrieved.
     * @return An {@link Optional} containing the plan project schedule, or
     * {@link Optional#empty()} if no schedule exists for the plan project.
     */
    @Nonnull
    public synchronized Optional<PlanProjectSchedule> getPlanProjectSchedule(final long planProjectId) {
        return Optional.ofNullable(planProjectTasks.get(planProjectId));
    }

    /**
     * Load all the plan projects from plan_project table, and schedule them.
     *
     * @param scheduleExecutor Thread Pool TaskScheduler
     */
    private void initializePlanProjectSchedule(final ThreadPoolTaskScheduler scheduleExecutor) {
        List<PlanProject> planProjects = planProjectDao.getAllPlanProjects();
        planProjects.forEach(planProject -> {
            if (!planProject.hasPlanProjectInfo()
                    || !planProject.getPlanProjectInfo().hasRecurrence()) {
                // Skip any non-scheduled projects like cloud migration plan projects.
                logger.trace("Skipping non-recurrence project {} from schedule.",
                        planProject.getPlanProjectId());
                return;
            }
            try {
                setPlanProjectSchedule(planProject.getPlanProjectId());
            } catch (PlanProjectNotFoundException e) {
                logger.error("Failed to create cron task for plan project:" +
                        planProject.getPlanProjectInfo().getName() +
                        " due to: " + e.getMessage());
            } catch (PlanProjectInfoNotFoundException e) {
                logger.error("Failed to create cron task for plan project:" +
                        planProject.getPlanProjectInfo().getName() +
                        " due to: " + e.getMessage());
            } catch (RuntimeException e) {
                // Catch all runtime exceptions to ensure exceptions will not prevent scheduling of subsequent tasks
                logger.error("Failed to create cron task for plan project:" +
                        planProject.getPlanProjectInfo().getName() +
                        " due to: " + e.getMessage());
            }
        });
        logger.info("Plan scheduler found " + planProjects.size()
                + " recurring plans in plan_project table during initialization.");
    }

    /**
     * Create cron trigger based on provided PlanProjectInfo.
     * Example cron String:
     * run daily at 7PM, "0 0 19 * * *".
     * run weekly on Monday at 7PM, "0 0 19 ? * 1".
     * run monthly on 1st at 7PM, "0 0 19 1 * ?".
     *
     * The return will be empty when 'Recurrence' is not available in PlanProjectInfo.
     * @param planProjectInfo the plan project info
     * @return Optional<Trigger> the optional Trigger, it's empty when 'Recurrence' is not available
     */
    @Nonnull
    public static Optional<Trigger> createCronTrigger(@Nonnull final PlanProjectInfo planProjectInfo) {
        if (planProjectInfo.hasRecurrence()) {
            Recurrence.Schedule schedule = planProjectInfo.getRecurrence().getSchedule();
            Recurrence.TimeOfRun timeOfRun = planProjectInfo.getRecurrence().getTimeOfRun();
            // build hour, e.g.  "0 0 19"
            StringBuilder cronString = new StringBuilder("0 0 " + timeOfRun.getHour() + " ");

            if (schedule.hasMonthly()) {
                // build Monthly, e.g. "0 0 19 1 * ?"
                cronString.append(schedule.getMonthly().getDayOfMonth());
                cronString.append(" * ?");
            } else if (schedule.hasWeekly()) {
                // build Weekly, e.g. "0 0 19 ? * 1"
                cronString.append("? * ");
                cronString.append(schedule.getWeekly().getDayOfWeek());
            } else if (schedule.hasDaily()) {
                // build Daily, e.g. "0 0 19 * * *"
                cronString.append("* * *");
            } else {
                throw new IllegalArgumentException("PlanProjectInfo has invalid schedule defined: "
                        + schedule);
            }
            Trigger trigger = new CronTrigger(cronString.toString());
            return Optional.of(trigger);
        }
        return Optional.empty();
    }

    /**
     * This method executes a scheduled recurring plan.
     * Passed via lambda to the scheduleExecutor.
     *
     * @param planProject The plan project to be executed.
     */
    @VisibleForTesting
    void executePlan(final PlanProject planProject) {
        planProjectExecutor.executePlan(planProject, planProject.getPlanProjectInfo().getHandleFailure());
    }

}
