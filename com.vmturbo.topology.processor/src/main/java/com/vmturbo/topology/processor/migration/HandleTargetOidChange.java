package com.vmturbo.topology.processor.migration;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Abstract class for migrating entries in the targetspec_oid table after we change the way some
 * target identifying fields are extracted from the target spec.  This class will find all probes
 * that use any of the specified fields and repopulate the oid table for any targets of those probes
 * using the new extraction scheme.
 */
public abstract class HandleTargetOidChange extends AbstractMigration {
    protected final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    private final TargetStore targetStore;

    private final IdentityStore<TargetSpec> identityStore;

    private final TargetDao targetDao;

    /**
     * Create the migration object.
     *
     * @param probeStore used to determine which probe types have "address" as target identifying
     *                   field.
     * @param targetStore to provide targets and target specs for updating identity store
     * @param idStore identity store to update by replacing ip address by the value the user put in
     *                the "address" field.
     * @param targetDao the target store for deleting targets made redundant by the Oid generation
     *                changes
     */
    public HandleTargetOidChange(@Nonnull ProbeStore  probeStore,
            @Nonnull TargetStore targetStore,
            @Nonnull IdentityStore<TargetSpec> idStore,
            @Nonnull TargetDao targetDao) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.identityStore = Objects.requireNonNull(idStore);
        this.targetDao = Objects.requireNonNull(targetDao);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("Starting target OID migration for targets of probes using one or more of "
                + getImpactedAccountDefKeys().stream().collect(Collectors.joining(", "))
                + " to determine target OID.");

        // Force initialization of the probe and target store (if applicable), so the non-migrated
        // data is loaded from Consul into their local state.
        if (probeStore instanceof RequiresDataInitialization) {
            try {
                ((RequiresDataInitialization)probeStore).initialize();
            } catch (InitializationException e) {
                String msg = "Failed to initialize probe store with error: " + e;
                logger.error(msg, e);
                return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
            }
        }

        if (targetStore instanceof RequiresDataInitialization) {
            try {
                ((RequiresDataInitialization)targetStore).initialize();
            } catch (InitializationException e) {
                String msg = "Failed to initialize target store with error: " + e;
                logger.error(msg, e);
                return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
            }
        }

        final Set<String> impactedAccountFields = getImpactedAccountDefKeys();
        // Get the set of all probes that use any of the fields in getImpactedAccountDefKeys
        // as a target identifying field
        final Set<Long> relevantProbeTypes = probeStore.getProbes().entrySet().stream()
                .filter(entry -> entry.getValue().getTargetIdentifierFieldList().stream()
                        .anyMatch(impactedAccountFields::contains))
                .map(Entry::getKey)
                .collect(Collectors.toSet());
        // Get all targets of these probes and update the OID cache and DB based on the new OID
        // matching attributes.
        final Map<Long, TargetSpec> targetToSpecMap = new HashMap<>();
        final Set<Long> duplicateTargets = new HashSet<>();
        final Set<Long> temporaryOids = new HashSet<>();
        relevantProbeTypes.stream()
                .forEach(id -> {
                    Set<Long> existingTargets = new HashSet<>();
                    // Go through the targets by probe type in increasing order of target ID within
                    // each probe type. If two targets are considered the same wrt the new Oid
                    // generation regime, mark the target with the higher Oid for deletion.
                    // Finally, update the Oid data structure and DB with the new attribute values
                    // for each remaining target of impacted probe types.
                    targetStore.getProbeTargets(id).stream()
                            .sorted(Comparator.comparingLong(Target::getId))
                            .forEach(target -> {
                                try {
                                    IdentityStoreUpdate<TargetSpec> idUpdate = identityStore
                                            .fetchOrAssignItemOids(Collections
                                                    .singletonList(target.getSpec()));
                                    final long newOid = idUpdate.getOldItems().isEmpty()
                                            ? idUpdate.getNewItems().get(target.getSpec())
                                            : idUpdate.getOldItems().get(target.getSpec());
                                    if (existingTargets.contains(newOid)) {
                                        duplicateTargets.add(target.getId());
                                    } else {
                                        existingTargets.add(newOid);
                                        targetToSpecMap.put(target.getId(), target.getSpec());
                                        // Anytime we add an existing target to our existingTarget
                                        // set, we need to remove the oid that matches it if it is
                                        // different than the target ID because otherwise, when we
                                        // go to push the new OID information to the store, it will
                                        // be rejected as an existing OID will have the same
                                        // matching criteria as the updated target.
                                        if (newOid != target.getId()) {
                                            temporaryOids.add(newOid);
                                        }
                                    }
                                } catch (IdentityStoreException e) {
                                    logger.error("Error obtaining OID for target {}({})",
                                            target.getDisplayName(), target.getId(), e);
                                }
                            });
                });
        logger.info("Updating OID attributes for {} targets from {} probe types.",
                targetToSpecMap.size(), relevantProbeTypes.size());
        try {
            // first remove OIDs for the duplicate targets and OIDs that were just created and won't
            // be used (since we will save all remaining targets with their existing OIDs)
            identityStore.removeItemOids(Sets.union(temporaryOids, duplicateTargets));
            // update the OID information based on new extraction regime
            identityStore.updateItemAttributes(targetToSpecMap);
        } catch (IdentityStoreException e) {
            String msg = "Error while updating target OIDs: " + e;
            logger.error(msg, e);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        } catch (IdentifierConflictException e) {
            String msg = "Oid clash while updating target OIDs.";
            logger.error(msg, e);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }
        if (!duplicateTargets.isEmpty()) {
            logger.info("Removing {} duplicate targets: {}", duplicateTargets.size(),
                    duplicateTargets.stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(", ")));
            duplicateTargets.forEach(targetDao::remove);
        }
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100,
                "Successfully migrated " + targetToSpecMap.size() + " targets.");
    }

    /**
     * Get the set of account definition fields that are impacted by this migration.  These are
     * target identifying fields that need to be handled by this migration.  This migration will
     * find all probes that have any of these fields as target identifying values and recalculate
     * OIDs for all targets of those probes.
     *
     * @return Set of Strings giving names of account fields whose handling in target OID generation
     * has changed.
     */
    protected abstract Set<String> getImpactedAccountDefKeys();
}
