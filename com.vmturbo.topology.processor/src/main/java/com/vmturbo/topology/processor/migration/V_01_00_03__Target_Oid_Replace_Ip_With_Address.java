package com.vmturbo.topology.processor.migration;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.platform.sdk.common.util.AccountDefEntryConstants;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * We previously made a change that converted all address fields to IP addresses when generating
 * attribute lists for determining target OIDs.  This created problems for some probe types as IP
 * address is not consistent over time and this allowed the same target to be added twice.  We
 * reverted this change and now must update the identity store and the DB that backs it to reflect
 * the correct value for address fields.  We do this by generating a map of target OID to TargetSpec
 * and updating the identity store, causing the matching attributes for each OID to be updated
 * according to the new scheme.  For efficiency, we skip targets that come from probe types that
 * don't have "address" as a target identifying field.
 */
public class V_01_00_03__Target_Oid_Replace_Ip_With_Address extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    private final TargetStore targetStore;

    private final IdentityStore<TargetSpec> identityStore;

    /**
     * Create the migration object.
     *
     * @param probeStore used to determine which probe types have "address" as target identifying
     *                   field.
     * @param targetStore to provide targets and target specs for updating identity store
     * @param idStore identity store to update by replacing ip address by the value the user put in
     *                the "address" field.
     */
    public V_01_00_03__Target_Oid_Replace_Ip_With_Address(@Nonnull ProbeStore  probeStore,
                                                          @Nonnull TargetStore targetStore,
                                                          @Nonnull IdentityStore<TargetSpec> idStore)
    {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.identityStore = Objects.requireNonNull(idStore);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("Starting target OID migration.");

        // Force initialization of the probe and target store (if applicable), so the non-migrated
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

        // Get the set of all probes that use "address" as a target identifying field
        final Set<Long> relevantProbeTypes = probeStore.getProbes().entrySet().stream()
            .filter(entry -> entry.getValue().getTargetIdentifierFieldList()
                .contains(AccountDefEntryConstants.ADDRESS_FIELD))
            .map(Entry::getKey)
            .collect(Collectors.toSet());
        // Get all targets of these probes and update the OID cache and DB based on the new OID
        // matching attributes.
        final Map<Long, TargetSpec> targetToSpecMap = relevantProbeTypes.stream()
            .flatMap(id -> targetStore.getProbeTargets(id).stream())
            .collect(Collectors.toMap(Target::getId, Target::getSpec));
        logger.info("Updating OID attributes for {} targets from {} probe types.",
            targetToSpecMap.size(), relevantProbeTypes.size());
        try {
            identityStore.updateItemAttributes(targetToSpecMap);
        } catch (IdentityStoreException e) {
            String msg = "Error while updating target OIDs.";
            logger.error(msg, e);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        } catch (IdentifierConflictException e) {
            String msg = "Oid clash while updating target OIDs.";
            logger.error(msg, e);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100,
            "Successfully migrated " + targetToSpecMap.size() + " targets.");
    }
}
