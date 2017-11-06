package com.vmturbo.plan.orchestrator.scheduled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.Recurrence;
import com.vmturbo.common.protobuf.plan.PlanDTO.Recurrence.TimeOfRun;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
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

    /**
     * Create a {@link PlanProjectScheduler} for scheduling plan project.
     *
     * @param planProjectDao plan project dao
     * @param scheduleExecutor The executor to be used for scheduling tasks.
     */
    public PlanProjectScheduler(@Nonnull final PlanProjectDao planProjectDao,
                                @Nonnull final ThreadPoolTaskScheduler scheduleExecutor) {
        this.schedulerExecutor = Objects.requireNonNull(scheduleExecutor);
        this.planProjectDao = planProjectDao;
        planProjectTasks = new HashMap<>();
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
            PlanDTO.PlanProject project = planProjectDao.getPlanProject(planProjectId).orElseThrow(
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
        List<PlanDTO.PlanProject> planProjects = planProjectDao.getAllPlanProjects();
        planProjects.forEach(this::scheduleProject);
        logger.info("Plan scheduler found " + planProjects.size()
                + " recurring plans in plan_project table.");
    }

    /**
     * PlanSchedule plan project:
     * 1. create trigger {@link Trigger} base on recurrence defined in PlanProjectInfo.
     * 2. schedule the plan project.
     * 3. add plan project id and schedule to planProjectTasks map.
     *
     * @param planProject the plan project to be scheduled
     */
    private void scheduleProject(PlanDTO.PlanProject planProject) {
        // adding try/catch block to ensure exception will not prevent scheduling subsequent tasks.
        try {
            Optional<Trigger> trigger = createCronTrigger(planProject.getPlanProjectInfo());
            trigger.ifPresent(cronTrigger -> {
                        final ScheduledFuture<?> scheduledTask = schedulerExecutor.schedule(
                                () -> executePlan(planProject), cronTrigger);
                        final PlanProjectSchedule schedule =
                                new PlanProjectSchedule(scheduledTask, planProject.getPlanProjectId());
                        planProjectTasks.put(planProject.getPlanProjectId(), schedule);
                    }
            );
        } catch (RuntimeException e) {
            logger.error("Failed to create cron task for plan project:" +
                    planProject.getPlanProjectInfo().getPlanProjectInfoName() +
                    " due to: " + e.getMessage());
        }
        logger.info("Plan scheduler successfully scheduled plan: " +
                planProject.getPlanProjectInfo().getPlanProjectInfoName());
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
        if (planProjectInfo.hasPlanProjectInfoRecurrence()) {
            Recurrence.Schedule schedule = planProjectInfo.getPlanProjectInfoRecurrence().getSchedule();
            TimeOfRun timeOfRun = planProjectInfo.getPlanProjectInfoRecurrence().getTimeOfRun();
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
     * A helper method that executes scheduled recurring plan.
     * Passed via lambda to the scheduleExecutor.
     * TODO: Will be implemented with OM-26167.
     *
     * @param planProject The plan project to be executed.
     */
    private void executePlan(final PlanDTO.PlanProject planProject) {

    }
}
