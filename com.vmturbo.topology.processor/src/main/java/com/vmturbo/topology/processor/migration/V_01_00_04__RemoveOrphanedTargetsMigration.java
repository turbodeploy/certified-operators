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
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Migration to remove orphaned targets left by a combination of incorrect migrations and missing
 * migrations when previous changes to targets were made.  Between 7.17 and 7.21 we made changes
 * that changed how target OIDs were calculated without writing migrations and changed the way
 * derived targets were tracked with an incorrect migration.  This has created a situation where
 * it's possible, for example, to either have multiple storage browsing targets for a single VC
 * target, or even have a Storage Browsing target remain after a VC target is deleted.  These
 * derived targets persist because they have no relationship with the parent target, thus they are
 * not cleaned up when the parent's discovery response is processed.  Furthermore, there is no
 * simple way to manually delete them, as the API does not allow direct deletion of derived targets.
 * This migration removes any derived targets that don't have parents.
 */
public class V_01_00_04__RemoveOrphanedTargetsMigration extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    private final TargetStore targetStore;

    private final ProbeStore probeStore;

    private final TargetDao targetDao;

    /**
     * Constructor for the migration.
     *
     * @param targetStore Needed to figure out which targets are orphaned.
     * @param probeStore Needed for determining which probes are derived.
     * @param targetDao Needed to delete orphaned targets.
     */
    public V_01_00_04__RemoveOrphanedTargetsMigration(@Nonnull TargetStore targetStore,
            @Nonnull ProbeStore probeStore, @Nonnull TargetDao targetDao) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetDao = Objects.requireNonNull(targetDao);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
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
}
