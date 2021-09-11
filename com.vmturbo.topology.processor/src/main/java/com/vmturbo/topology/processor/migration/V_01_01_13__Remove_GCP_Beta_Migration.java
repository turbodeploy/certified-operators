package com.vmturbo.topology.processor.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This migration removes GCP Beta probe/targets, as well as GCP Cost probe/targets derived from GCP beta.
 */
public class V_01_01_13__Remove_GCP_Beta_Migration extends AbstractMigration {

    protected static final String GCP_BETA_PROBE_TYPE = "GCP";
    protected static final String GCP_COST_PROBE_TYPE = SDKProbeType.GCP_COST.getProbeType();

    private final Logger logger = LogManager.getLogger();
    private final KeyValueStore keyValueStore;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    /**
     * Constructor.
     *
     * @param keyValueStore key value store.
     * @param probeStore  probe store.
     * @param targetStore target store.
     */
    public V_01_01_13__Remove_GCP_Beta_Migration(@Nonnull KeyValueStore keyValueStore,
            @Nonnull ProbeStore probeStore, @Nonnull TargetStore targetStore) {
        this.keyValueStore = keyValueStore;
        this.probeStore = probeStore;
        this.targetStore = targetStore;
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        try {
            probeStore.initialize();
            targetStore.initialize();
        } catch (InitializationException e) {
            final String msg = "Failed to initialize probe or target store with error ";
            logger.error(msg + e);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg + e.getMessage());
        }
        final List<Long> probeIdsToRemove = getProbeIdsToRemove();
        if (probeIdsToRemove.isEmpty()) {
            // Directly skip migration if no GCP Beta probe found.
            return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, "GCP Beta probe doesn't exist.");
        }
        final List<Target> targetsToRemove = new ArrayList<>();
        probeIdsToRemove.forEach(probeId -> {
            // Remove probe entries
            keyValueStore.removeKeysWithPrefix(ProbeStore.PROBE_KV_STORE_PREFIX + probeId);
            targetsToRemove.addAll(targetStore.getProbeTargets(probeId));
        });
        final String probeRmMsg = String.format("Successfully removed GCP Beta and Cost probes %s. Starting to remove targets.",
                probeIdsToRemove.stream().map(probeId -> String.format("%d ", probeId)).collect(Collectors.joining("|")));
        logger.info(probeRmMsg);
        updateMigrationProgress(MigrationStatus.RUNNING, 40, probeRmMsg);
        // Remove target entries
        targetsToRemove.forEach(target -> keyValueStore.removeKeysWithPrefix(TargetStore.TARGET_KV_STORE_PREFIX + target.getId()));
        final String targetRmMsg = "Successfully removed GCP Beta and Cost targets "
                + targetsToRemove.stream().map(target -> String.format("%s (%d)", target.getDisplayName(), target.getId()))
                .collect(Collectors.joining("|"));
        logger.info(targetRmMsg);
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, targetRmMsg);
    }

    @Nonnull
    private List<Long> getProbeIdsToRemove() {
        final Long gcpBetaProbeId = probeStore.getProbeIdForType(GCP_BETA_PROBE_TYPE).orElse(null);
        if (gcpBetaProbeId == null) {
            logger.info("No GCP Beta probe found.");
            return Collections.emptyList();
        }
        final List<Long> probeIdsToRemove = new ArrayList<>();
        probeIdsToRemove.add(gcpBetaProbeId);
        // If found GCP Beta probe, remove both GCP Beta and GCP Cost probes, because GCP Cost probe
        // is derived from GCP Beta.
        final Long gcpCostProbeId = probeStore.getProbeIdForType(GCP_COST_PROBE_TYPE).orElse(null);
        if (gcpCostProbeId == null) {
            logger.warn("There is no cost probe associated with GCP Beta probe {}", gcpBetaProbeId);
        } else {
            probeIdsToRemove.add(gcpCostProbeId);
        }
        return probeIdsToRemove;
    }
}