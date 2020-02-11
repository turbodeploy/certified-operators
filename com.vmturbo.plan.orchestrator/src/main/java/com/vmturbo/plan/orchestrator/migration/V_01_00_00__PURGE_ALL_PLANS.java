package com.vmturbo.plan.orchestrator.migration;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;

/**
 * This migration will purge all existing plans when upgrading to 7.21 which
 * includes Plan Overhaul, multi-tenancy and other breaking changes/improvements.
 *
 */
public class V_01_00_00__PURGE_ALL_PLANS implements Migration {

    /**
     * For logging migration status.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Used to list and delete all existing (obsolete) plans.
     *
     * <p>Will also make remote calls to other components to delete plan data.</p>
     */
    private final PlanDao planDao;

    /**
     * Used to delete scenarios that correspond to plans being deleted.
     */
    private final ScenarioDao scenarioDao;

    /**
     * For reporting migration progress.
     */
    //TODO: Improve this with an abstract superclass.
    @GuardedBy("migrationInfoLock")
    private final MigrationProgressInfo.Builder migrationInfo = MigrationProgressInfo.newBuilder();

    /**
     * For synchronizing access to the migrationInfo.
     */
    private final Object migrationInfoLock = new Object();

    /**
     * Create an instance of the Purge all Plans migration.
     *
     * @param planDao to delete plan data
     * @param scenarioDao to delete scenario data
     */
    public V_01_00_00__PURGE_ALL_PLANS(@Nonnull final PlanDao planDao,
                                       @Nonnull final ScenarioDao scenarioDao) {
        this.planDao = Objects.requireNonNull(planDao);
        this.scenarioDao = Objects.requireNonNull(scenarioDao);
    }

    /**
     * Retrieve the current status of the migration.
     *
     * <p>See {@link MigrationStatus} for the list of states.</p>
     *
     * @return the current {@link MigrationStatus}
     */
    @Override
    public MigrationStatus getMigrationStatus() {
        synchronized (migrationInfoLock) {
            return migrationInfo.getStatus();
        }
    }

    /**
     * Retrieve the current info about the migration.
     *
     * @return the current {@link MigrationProgressInfo}
     */
    @Override
    public MigrationProgressInfo getMigrationInfo() {
        synchronized (migrationInfoLock) {
            return migrationInfo.build();
        }
    }

    /**
     * Start the migration, deleting all obsolete plan topologies.
     *
     * @return {@link MigrationProgressInfo} describing the details
     * of the migration
     */
    @Override
    public MigrationProgressInfo startMigration() {
        logger.info("Starting migration...");
        synchronized (migrationInfoLock) {
            migrationInfo.setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.RUNNING);
        }
        return deleteOldPlans();
    }

    private MigrationProgressInfo deleteOldPlans() {
        try {
            // Query plan_instance db to get all the planIds older than the
            // current time. Then delete them one by one.
            final int batchSize = 100;
            LocalDateTime expirationDate = LocalDateTime.now();
            List<PlanInstance> expiredPlans = planDao.getOldPlans(expirationDate, batchSize);
            logger.info("Deleting all plans older than {}", expirationDate);
            while (!expiredPlans.isEmpty()) {
                for (PlanDTO.PlanInstance plan : expiredPlans) {
                    try {
                        // Delete plan data across all components--remote calls to other components
                        // will be made by planDao.
                        logger.info("Deleting obsolete plan: {}", plan.getPlanId());
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
            }
        } catch (RuntimeException e) {
            logger.error(e.getMessage(), e);
            return migrationFailed(e.getMessage());
        }
        return migrationSucceeded();
    }

    /**
     * Generate a migrationInfo indicating a successful migration.
     *
     * @return migrationInfo indicating a successful migration
     */
    @Nonnull
    private MigrationProgressInfo migrationSucceeded() {
        return migrationInfo
            .setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.SUCCEEDED)
            .setCompletionPercentage(100)
            .build();
    }

    /**
     * Generate a migrationInfo indicating a failed migration.
     *
     * @param errorMessage a message indicating why the migration failed
     * @return migrationInfo indicating a failed migration
     */
    @Nonnull
    private MigrationProgressInfo migrationFailed(@Nonnull String errorMessage) {
        return migrationInfo
            .setStatus(com.vmturbo.common.protobuf.common.Migration.MigrationStatus.FAILED)
            .setStatusMessage("Migration failed: " + errorMessage)
            .build();
    }
}
