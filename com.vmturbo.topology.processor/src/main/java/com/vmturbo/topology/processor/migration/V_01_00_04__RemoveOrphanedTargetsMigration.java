package com.vmturbo.topology.processor.migration;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.topology.processor.probes.ProbeStore;
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

    MigrationOrphanedTargetsRemover migrationOrphanedTargetsRemover;

    /**
     * Constructor for the migration.
     *
     * @param targetStore Needed to figure out which targets are orphaned.
     * @param probeStore Needed for determining which probes are derived.
     * @param targetDao Needed to delete orphaned targets.
     */
    public V_01_00_04__RemoveOrphanedTargetsMigration(@Nonnull TargetStore targetStore,
            @Nonnull ProbeStore probeStore, @Nonnull TargetDao targetDao) {
        this.migrationOrphanedTargetsRemover = new MigrationOrphanedTargetsRemover(targetStore,
            probeStore, targetDao);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("Starting migration for 'first generation orphaned targets' removal");
        return migrationOrphanedTargetsRemover.removeOrphanedTargets();
    }
}
