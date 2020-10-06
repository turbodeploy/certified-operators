package com.vmturbo.topology.processor.migration;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Class for removing 'first and second generation' orphaned targets.
 */
public class MigrationOrphanedTargetsRemover {

    private final Logger logger = LogManager.getLogger();

    private final TargetStore targetStore;

    private final ProbeStore probeStore;

    private final TargetDao targetDao;

    /**
     * Basic constructor for the class.
     *
     * @param targetStore used for getting all the targets.
     * @param probeStore used for getting probes data.
     * @param targetDao used for updating the targets db.
     */
    public MigrationOrphanedTargetsRemover(@Nonnull TargetStore targetStore,
                                           @Nonnull ProbeStore probeStore, @Nonnull TargetDao targetDao) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetDao = Objects.requireNonNull(targetDao);
    }

    /**
     * Will remove the orphaned targets from the db, this will only remove first level orphans.
     *
     * @return MigrationStatus.SUCCEEDED if successful or else FAILURE
     */
    public MigrationProgressInfo removeOrphanedTargets() {
        logger.info("Starting deletion of orphaned targets.");

        // Force initialization of the target store (if applicable), so the non-migrated
        // data is loaded from Consul into their local state.
        if (probeStore instanceof RequiresDataInitialization) {
            try {
                ((RequiresDataInitialization)probeStore).initialize();
            } catch (InitializationException e) {
                String msg = "Failed to initialize probe store with error: " + e.getMessage();
                logger.error("{}", msg, e);
                return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
            }
        }

        if (targetStore instanceof RequiresDataInitialization) {
            try {
                ((RequiresDataInitialization)targetStore).initialize();
            } catch (InitializationException e) {
                String msg = "Failed to initialize target store with error: " + e.getMessage();
                logger.error("{}", msg, e);
                return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
            }
        }

        // Create a set of all derived target IDs that have parents.
        Set<Long> derivedTargetsWithParents = targetStore.getAll().stream()
            .flatMap(target -> targetStore.getDerivedTargetIds(target.getId()).stream())
            .collect(Collectors.toSet());

        // Delete targets that are derived targets that don't have parents
        Set<Target> orphanedTargets = Sets.newHashSet();
        targetStore.getAll().stream()
            .filter(target -> target.getProbeInfo().getCreationMode() == CreationMode.DERIVED)
            .filter(target -> !derivedTargetsWithParents.contains(target.getId()))
            .forEach(target -> {
                logger.info("Deleting orphaned target {}({}) of probe type {}.",
                    target.getDisplayName(), target.getId(),
                    target.getProbeInfo().getProbeType());

                // It is safe to delete the target directly from the TargetDao because we know
                // initialization of the TargetStore will be run after the migration, and the
                // TargetStore will be re-initialized from the TargetDao.
                targetDao.remove(target.getId());
                orphanedTargets.add(target);
            });
        final String msg = orphanedTargets.size() > 0 ? "Successfully removed orphaned targets: "
            + orphanedTargets.stream()
            .map(target -> String.format("%s (%d)", target.getDisplayName(), target.getId()))
            .collect(Collectors.joining("|")) : "No orphaned targets found.";
        logger.info("Remove Orphaned Targets Migration: " + msg);
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
    }

    /**
     *  Update the migrationInfo and return a new MigrationProgressInfo
     *  with the updated status.
     *
     * @param status updated status of the migration in progress
     * @param completionPercentage a decimal value from 0.0 to 100.0
     * @param msg a message to associate with the current status
     * @return migrationInfo indicating the updated status of the migration
     */
    @Nonnull
    private MigrationProgressInfo updateMigrationProgress(@Nonnull MigrationStatus status,
                                                            @Nonnull float completionPercentage,
                                                            @Nonnull String msg) {
        return MigrationProgressInfo.newBuilder()
            .setStatus(status)
            .setCompletionPercentage(completionPercentage)
            .setStatusMessage(msg)
            .build();
    }
}
