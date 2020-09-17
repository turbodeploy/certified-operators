package com.vmturbo.plan.orchestrator.scheduled;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;

/**
 * Task to delete old/expired plans and its associated information.
 *
 **/
public class PlanDeletionTask {

    private final Logger logger = LogManager.getLogger(getClass());

    private final CronTrigger cronTrigger;

    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;

    private final SettingServiceBlockingStub settingsServiceClient;

    private final DSLContext dsl;

    private final PlanDao planDao;

    private final PlanProjectDao planProjectDao;

    private final ScenarioDao scenarioDao;

    private final int batchSize;

    private final int delayBetweenDeletesInSeconds;

    PlanDeletionTask(@Nonnull final SettingServiceBlockingStub settingsServiceClient,
                     @Nonnull PlanDao planDao,
                     @Nonnull PlanProjectDao planProjectDao,
                     @Nonnull ScenarioDao scenarioDao,
                     @Nonnull final DSLContext dsl,
                     @Nonnull final ThreadPoolTaskScheduler threadPoolTaskScheduler,
                     @Nonnull CronTrigger cronTrigger,
                     int batchSize,
                     int delayBetweenDeletesInSeconds) {

        this.settingsServiceClient = settingsServiceClient;
        this.dsl = dsl;
        this.cronTrigger = cronTrigger;
        this.threadPoolTaskScheduler = threadPoolTaskScheduler;
        this.planDao = planDao;
        this.planProjectDao = planProjectDao;
        this.scenarioDao = scenarioDao;
        this.batchSize = batchSize;
        this.delayBetweenDeletesInSeconds = delayBetweenDeletesInSeconds;
    }

    /**
     * Start the plan deletion task in the background.
     */
    public void start() {

        logger.info("Start the plan deletion task with the cron settings {}, batchSize: {}",
            cronTrigger.getExpression(), batchSize);
        threadPoolTaskScheduler.schedule(this::deleteOldPlans, cronTrigger);
    }

    /**
     *  Gets the plan deletion retention value from group component and based
     *  on that deletes all the old/expired plans and associated plan data in
     *  other components.
     */
    private void deleteOldPlans() {
        try {

            logger.info("Starting to delete old plans");
            // We query the retention settings value during every run of this
            // deletion task. This way any updates to the retention setting
            // values are picked up.
            GetSingleGlobalSettingRequest settingRequest =
                GetSingleGlobalSettingRequest.newBuilder()
                    .setSettingSpecName(GlobalSettingSpecs.PlanRetentionDays.getSettingName())
                    .build();

            final GetGlobalSettingResponse response =
                    settingsServiceClient.getGlobalSetting(settingRequest);
            // If the response doesn't have any value, exit. We don't want to
            // continue with any defaul value.
            if (!response.hasSetting() || !response.getSetting().hasNumericSettingValue()) {
                logger.warn("No plan retention value set in the response. Quitting deletion task");
                return;
            }
            long retentionDays = (long) response.getSetting().getNumericSettingValue().getValue();
            // Query plan_instance db to get all the planIds older than the
            // retentionDays. Then delete them one by one.
            LocalDateTime expirationDate = LocalDateTime.now().minusDays(retentionDays);
            List<PlanDTO.PlanInstance> expiredPlans = planDao.getOldPlans(expirationDate, batchSize);
            logger.info("Deleting all plans older than {}", expirationDate);
            while (!expiredPlans.isEmpty()) {
                for (PlanDTO.PlanInstance plan : expiredPlans) {
                    try {
                        // TODO: karthikt -  Deleting the plan related data one at a
                        // time is in-efficient as it leads lot of small disk
                        // writes(on different data stores). Would be good to
                        // support batch plan deletion. It's complicated by
                        // the fact that the data is distributed amongst
                        // different components.
                        if (plan.hasPlanProjectId()) {
                            deletePlanProject(plan.getPlanProjectId());
                        } else {
                            logger.debug("Deleting plan: {}", plan.getPlanId());
                            planDao.deletePlan(plan.getPlanId());
                            scenarioDao.deleteScenario(plan.getScenario().getId());
                        }
                    } catch (NoSuchObjectException ex) {
                        // Ignore this exception as it doesn't matter.
                        // This exception can happen if a plan is explicitly deleted
                        // from the UI after we query the plan_instance table
                        // but before the deletePlan() call is invoked.
                    }
                }
                expiredPlans = planDao.getOldPlans(expirationDate, batchSize);
                try {
                    Thread.sleep(delayBetweenDeletesInSeconds);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while deleting old plans", e);
                    Thread.currentThread().interrupt();
                }
            }
            deleteOrphanPlanProjects();
        } catch (RuntimeException e) {
            logger.error("Exception while deleting old plans", e);
        }
    }

    /**
     * Because of a previous bug, we ended up deleting records from plan_instance table, but not
     * from the plan_project table. Such plan projects cause issue when loading the plan UI page.
     * This method finds such orphan plan projects, deletes the plan project itself, so that next
     * time UI is loaded, we don't get the error.
     * It queries for all plan projects in DB, and if the project's main plan or related plan is
     * missing from DB, then the project is deleted.
     */
    private void deleteOrphanPlanProjects() {
        final List<PlanProject> projects = planProjectDao.getAllPlanProjects();
        for (final PlanProject planProject : projects) {
            final PlanProjectInfo projectInfo = planProject.getPlanProjectInfo();
            if (!projectInfo.hasMainPlanId()) {
                // This project doesn't have a main plan, so skip it.
                continue;
            }
            boolean isOrphanProject = false;
            final PlanInstance mainPlan = planDao.getPlanInstance(projectInfo.getMainPlanId())
                    .orElse(null);
            if (mainPlan == null) {
                // Main plan supposed to exist, but not there in DB, so clean up the project.
                isOrphanProject = true;
            } else {
                // Main plan exists, verify all related plans exist as well.
                for (Long planId : projectInfo.getRelatedPlanIdsList()) {
                    final PlanInstance relatedPlan = planDao.getPlanInstance(planId).orElse(null);
                    if (relatedPlan == null) {
                        isOrphanProject = true;
                        break;
                    }
                }
            }
            if (isOrphanProject) {
                logger.info("Deleting orphan project {} with missing plans.",
                        planProject.getPlanProjectId());
                try {
                    deletePlanProject(planProject.getPlanProjectId());
                } catch (NoSuchObjectException e) {
                    logger.warn("Unable to delete orphan plan project {}. Message: {}",
                            planProject.getPlanProjectId(), e.getMessage());
                }
            }
        }
    }

    /**
     * Deletes a project and its associated plans/scenarios from DB. The main plan of a project
     * and its scenario are deleted. Also deleted are any related plans of the project and their
     * scenarios. Finally the project itself is deleted.
     *
     * @param projectId Plan project to clean up.
     * @throws NoSuchObjectException Thrown if plan being deleted could not be found.
     */
    private void deletePlanProject(final long projectId) throws NoSuchObjectException {
        final Optional<PlanProject> optionalPlanProject = planProjectDao.getPlanProject(projectId);
        if (!optionalPlanProject.isPresent()) {
            logger.debug("Skipping non-existent plan project {}", projectId);
            return;
        }
        final PlanProject planProject = optionalPlanProject.get();
        final PlanProjectInfo projectInfo = planProject.getPlanProjectInfo();
        if (projectInfo.hasMainPlanId()) {
            final PlanInstance mainPlan = planDao.getPlanInstance(projectInfo.getMainPlanId())
                    .orElse(null);
            if (mainPlan != null) {
                logger.debug("Deleting main plan: {} and scenario {}.", mainPlan.getPlanId(),
                        mainPlan.getScenario().getId());
                planDao.deletePlan(mainPlan.getPlanId());
                scenarioDao.deleteScenario(mainPlan.getScenario().getId());
            }
            for (Long planId : projectInfo.getRelatedPlanIdsList()) {
                final PlanInstance relatedPlan = planDao.getPlanInstance(planId).orElse(null);
                if (relatedPlan != null) {
                    logger.debug("Deleting related plan: {} and scenario {}.",
                            relatedPlan.getPlanId(), relatedPlan.getScenario().getId());
                    planDao.deletePlan(relatedPlan.getPlanId());
                    scenarioDao.deleteScenario(relatedPlan.getScenario().getId());
                }
            }
        }
        logger.debug("Deleting plan project {}, name = {}, type = {}.", projectId,
                projectInfo.getName(), projectInfo.getType());
        planProjectDao.deletePlan(projectId);
    }
}
