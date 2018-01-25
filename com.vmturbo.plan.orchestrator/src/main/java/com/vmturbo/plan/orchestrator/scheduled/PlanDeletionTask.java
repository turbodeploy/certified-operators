package com.vmturbo.plan.orchestrator.scheduled;

import java.time.LocalDateTime;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
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

    private final ScenarioDao scenarioDao;

    private final int batchSize;

    private final int delayBetweenDeletesInSeconds;

    PlanDeletionTask(@Nonnull final SettingServiceBlockingStub settingsServiceClient,
                     @Nonnull PlanDao planDao,
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
        this.scenarioDao = scenarioDao;
        this.batchSize = batchSize;
        this.delayBetweenDeletesInSeconds = delayBetweenDeletesInSeconds;
    }

    /**
     * Start the plan deletion task in the background.
     */
    public void start() {

        logger.info("Start the plan deletion task with the cron settings {}", cronTrigger.getExpression());
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

            Setting planRetentionSetting = settingsServiceClient.getGlobalSetting(settingRequest);
            // If the response doesn't have any value, exit. We don't want to
            // continue with any defaul value.
            if (!planRetentionSetting.hasNumericSettingValue()) {
                logger.warn("No plan retention value set in the response. Quitting deletion task");
                return;
            }
            long retentionDays = (long) planRetentionSetting.getNumericSettingValue().getValue();
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
                        logger.debug("Deleting plan: {}", plan.getPlanId());
                        planDao.deletePlan(plan.getPlanId());
                        scenarioDao.deleteScenario(plan.getScenario().getId());
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
        } catch (RuntimeException e) {
            logger.error("Exception while deleting old plans", e);
        }
    }
}
