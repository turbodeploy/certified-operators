package com.vmturbo.plan.orchestrator.project;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject.PlanProjectStatus;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.project.PlanProjectStatusListener.PlanProjectStatusListenerException;

/**
 * Tracks the status of a plan project as each underlying plan goes through its stages.
 * Sends project notification updates, and project DB table is updated with current status.
 */
public class PlanProjectStatusTracker {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Id of the plan project.
     */
    private long projectId;

    /**
     * Plan project DAO to save status changes to DB.
     */
    private PlanProjectDao planProjectDao;

    /**
     * Sender used to notify status changes.
     */
    private PlanProjectNotificationSender sender;

    /**
     * Map to keep the status of each plan instance id and its status.
     */
    private Map<Long, PlanDTO.PlanInstance.PlanStatus> statusMap = new ConcurrentHashMap<>();

    /**
     * Creates a new tracker.
     *
     * @param projectId Id of the plan project.
     * @param planProjectDao Project DAO to save changes to DB.
     * @param sender Project notification sender.
     */
    public PlanProjectStatusTracker(long projectId, @Nonnull PlanProjectDao planProjectDao,
                                    @Nonnull PlanProjectNotificationSender sender) {
        this.projectId = projectId;
        this.planProjectDao = planProjectDao;
        this.sender = sender;
    }

    /**
     * Called when plan that is part of the project gets a new status.
     *
     * @param planInstanceId Plan id.
     * @param status New status.
     * @throws PlanProjectStatusListenerException Thrown on listen problem.
     * @throws IntegrityException Thrown on DB update issues.
     * @throws NoSuchObjectException Thrown on DB update issues.
     */
    public void updateStatus(long planInstanceId, PlanStatus status)
            throws PlanProjectStatusListenerException, IntegrityException, NoSuchObjectException {
        PlanStatus oldStatus = statusMap.get(planInstanceId);
        PlanProject project;
        if (oldStatus == null || oldStatus != status) {
            // at least one plan instance has status changed
            statusMap.put(planInstanceId, status);
            logger.info("Plan instance {} status {}", planInstanceId, status);
            boolean allSuccessful = true;
            boolean anyFailed = false;
            boolean anyRunning = false;
            for (PlanStatus eachPlanStatus : statusMap.values()) {
                allSuccessful &= eachPlanStatus == PlanStatus.SUCCEEDED;
                anyFailed |= eachPlanStatus == PlanStatus.FAILED;
                anyRunning |= eachPlanStatus == PlanStatus.RUNNING_ANALYSIS;
            }
            if (allSuccessful) {
                // all instances are succeeded, update plan project to be SUCCEEDED
                project = planProjectDao.updatePlanProject(projectId,
                        PlanProjectStatus.SUCCEEDED);
                sender.onPlanStatusChanged(project);
                logger.info("Plan project {} SUCCEEDED", projectId);
            } else if (anyFailed) {
                // at least one instance failed, , update plan project to be FAILED
                project = planProjectDao.updatePlanProject(projectId,
                        PlanProjectStatus.FAILED);
                sender.onPlanStatusChanged(project);
                logger.info("Plan project {} FAILED", projectId);
            } else if (anyRunning) {
                // at least one instance is running
                planProjectDao.updatePlanProject(projectId, PlanProjectStatus.RUNNING);
            }
        }
    }
}
