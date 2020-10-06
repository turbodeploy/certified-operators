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
 * migrations when previous changes to targets were made.
 * The migration will run after an iteration of 'first generation' orphans were removed and will
 * remove the 'second generation' orphans.
 * for example (Azure SP removed) -> (Azure Subscription is a first generation orphan) ->
 * (Azure cost is a second generation orphan)
 */
public class V_01_01_04__Fix_Orphaned_Target_Removal_Migration extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    MigrationOrphanedTargetsRemover migrationOrphanedTargetsRemover;

    /**
     * Constructor for the migration.
     *
     * @param targetStore Needed to figure out which targets are orphaned.
     * @param probeStore Needed for determining which probes are derived.
     * @param targetDao Needed to delete orphaned targets.
     */
    public V_01_01_04__Fix_Orphaned_Target_Removal_Migration(@Nonnull TargetStore targetStore,
                                                             @Nonnull ProbeStore probeStore, @Nonnull TargetDao targetDao) {
        this.migrationOrphanedTargetsRemover = new MigrationOrphanedTargetsRemover(targetStore,
            probeStore, targetDao);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("Starting migration for 'second generation orphaned targets' removal");
        return migrationOrphanedTargetsRemover.removeOrphanedTargets();
    }
}
