package com.vmturbo.topology.processor.migration;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 *  Migrate the probeInfo for VCenter Probes due to addition of new fields to the
 *  Account Definitions and Identity Metadata properties introduced by the implementation of
 *  Storage Browsing (wasted files detection) in XL.
 *  https://vmturbo.atlassian.net/browse/OM-44698
 *
 * Adds:
 *  1. New account definition "isStorageBrowsingEnabled".
 *  2. New entity identity metadata for VIRTUAL_VOLUME entity type.
 */
public class V_01_00_01__Vim_Probe_Storage_Browsing_Migration extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    private final IdentityServiceUnderlyingStore identityInMemoryStore;

    private final IdentityProvider identityProvider;

    // New Account Definition Entry for VC Storage Browsing flag
    private static final AccountDefEntry STORAGE_BROWSING_ACCOUNT_DEFINITION =
        AccountDefEntry.newBuilder()
            .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                .setName("isStorageBrowsingEnabled")
                .setDisplayName("Enable Datastore Browsing")
                .setDescription("Enable datastore browsing for this target.")
                .setVerificationRegex("(true|false)")
                .setIsSecret(false)
                .setPrimitiveValue(PrimitiveValue.BOOLEAN))
            .setMandatory(false)
            .setDefaultValue("true")
            .build();

    // New Entity Metadata for VIRTUAL_VOLUME entity type in VC probe
    private static final EntityIdentityMetadata VIRTUAL_VOLUME_METADATA =
        EntityIdentityMetadata.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .addNonVolatileProperties(PropertyMetadata.newBuilder()
                .setName("id")
                .build())
            .build();

    /**
     * Create a VC probe migration to add storage browsing metadata
     *
     * @param probeStore used to retrieve and update the probeInfo for the vCenter probe
     * @param identityInMemoryStore used to update the (in-memory) entity identity metadata
     * @param identityProvider used to update the (persistent) entity identity metadata
     */
    public V_01_00_01__Vim_Probe_Storage_Browsing_Migration(@Nonnull ProbeStore probeStore,
                                                            @Nonnull IdentityServiceUnderlyingStore identityInMemoryStore,
                                                            @Nonnull IdentityProvider identityProvider) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.identityInMemoryStore = Objects.requireNonNull(identityInMemoryStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        // Update the probe metadata with the new account value.
        logger.info("Starting migration of vCenter account values.");

        // Force initialization of the probe store, so the non-migrated
        // data is loaded from Consul into its local state.
        if (probeStore instanceof RequiresDataInitialization) {
            try {
                ((RequiresDataInitialization)probeStore).initialize();
            } catch (InitializationException e) {
                String msg = "Failed to initialize probe store with error: " + e.getMessage();
                logger.error("{}", msg, e);
                return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
            }
        }

        Optional<Long> probeId =
            probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType());
        if (!probeId.isPresent()) {
            String msg ="No vCenter probe to upgrade. Upgrade finished.";
            logger.info(msg);
            return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
        }

        Optional<ProbeInfo> oldProbeInfoOpt = probeStore.getProbe(probeId.get());

        // If there is no VM EntityMetadata, fail.
        if (!oldProbeInfoOpt.isPresent()) {
            String msg = "Missing vCenter probe info to upgrade. Upgrade aborted.";
            logger.error(msg);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }

        ProbeInfo oldProbeInfo = oldProbeInfoOpt.get();
        final int oldAccountValuesCount = oldProbeInfo.getAccountDefinitionList().size();
        if (oldAccountValuesCount != 3) {
            String msg = "vCenter probe info has unexpected number of account definitions: "
                + oldAccountValuesCount + ". Upgrade aborted.";
            logger.error(msg);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }

        // Prepare the updated ProbeInfo
        ProbeInfo.Builder newProbeInfo = oldProbeInfo.toBuilder();
        // Add the Storage Browsing account definition
        newProbeInfo.addAccountDefinition(3, STORAGE_BROWSING_ACCOUNT_DEFINITION);

        // Add the VIRTUAL_VOLUMES entity type
        newProbeInfo.addEntityMetadata(VIRTUAL_VOLUME_METADATA);

        logger.info("Updating to the new probeInfo {} for probeId: {}", newProbeInfo, probeId);
        try {
            probeStore.updateProbeInfo(newProbeInfo.build());
        } catch (ProbeException pe) {
            String msg = "Error while persisting new probeInfo {}";
            logger.error(msg, newProbeInfo, pe);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        final String progressMessage = "Added isStorageBrowsingEnabled flag to account definitions.";
        updateMigrationProgress(MigrationStatus.RUNNING, 50, progressMessage);

        // Reload the IdentityMetadataInMemory store so that it picks up the updated entries
        identityProvider.updateProbeInfo(newProbeInfo.build());
        identityInMemoryStore.reloadEntityDescriptors();

        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100,
            "Successfully migrated the probeInfo to include the isStorageBrowsingEnabled flag.");
    }
}
