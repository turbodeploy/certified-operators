package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_13__Remove_GCP_Beta_Migration.GCP_BETA_PROBE_TYPE;
import static com.vmturbo.topology.processor.migration.V_01_01_13__Remove_GCP_Beta_Migration.GCP_COST_PROBE_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Optional;

import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountTypeTest.FakeKeyValueStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test migration to remove probes and targets for GCP Beta and GCP Cost.
 */
public class V_01_01_13__Remove_GCP_Beta_MigrationTest {
    private V_01_01_13__Remove_GCP_Beta_Migration migration;
    private KeyValueStore keyValueStore;
    private ProbeStore probeStore;
    private TargetStore targetStore;

    private static final String GCP_BETA_PROBE_ID = "74108122065136";
    private static final String GCP_COST_PROBE_ID = "74108122050608";
    private static final String GCP_BETA_TARGET_ID = "74109384049936";
    private static final String GCP_COST_TARGET_ID = "74109385223232";

    /**
     * Set up.
     *
     * @throws InitializationException possible exception during probeStore or targetStore initialization
     */
    @Before
    public void setup() throws InitializationException {
        keyValueStore = new FakeKeyValueStore(new HashMap<>());
        probeStore = Mockito.mock(ProbeStore.class);
        targetStore = Mockito.mock(TargetStore.class);
        migration = new V_01_01_13__Remove_GCP_Beta_Migration(keyValueStore, probeStore, targetStore);
        doNothing().when(probeStore).initialize();
        doNothing().when(targetStore).initialize();
    }

    /**
     * If there is no GCP Beta probe, skip migration directly.
     */
    @Test
    public void testNoGCPBetaProbeFound() {
        when(probeStore.getProbeIdForType(GCP_BETA_PROBE_TYPE)).thenReturn(Optional.empty());
        final MigrationProgressInfo migrationResult = migration.doStartMigration();
        assertEquals(MigrationStatus.SUCCEEDED, migrationResult.getStatus());
        assertEquals("GCP Beta probe doesn't exist.", migrationResult.getStatusMessage());
    }

    /**
     * Test removing probes and targets for GCP Beta and GCP Cost.
     */
    @Test
    public void testRemovingGCPBetaProbesTargets() {
        // Key value store has GCP Beta probe, GCP Cost probe, 1 GCP Beta target and 1 GCP Cost target.
        keyValueStore.put(ProbeStore.PROBE_KV_STORE_PREFIX + GCP_BETA_PROBE_ID, "gcpBetaProbe");
        keyValueStore.put(ProbeStore.PROBE_KV_STORE_PREFIX + GCP_COST_PROBE_ID, "gcpCostProbe");
        keyValueStore.put(TargetStore.TARGET_KV_STORE_PREFIX + GCP_BETA_TARGET_ID, "gcpBetaTarget");
        keyValueStore.put(TargetStore.TARGET_KV_STORE_PREFIX + GCP_COST_TARGET_ID, "gcpCostTarget");
        Target gcpBetaTarget = Mockito.mock(Target.class);
        Target gcpCostTarget = Mockito.mock(Target.class);
        when(probeStore.getProbeIdForType(GCP_BETA_PROBE_TYPE)).thenReturn(Optional.of(Long.valueOf(GCP_BETA_PROBE_ID)));
        when(probeStore.getProbeIdForType(GCP_COST_PROBE_TYPE)).thenReturn(Optional.of(Long.valueOf(GCP_COST_PROBE_ID)));
        when(targetStore.getProbeTargets(Long.valueOf(GCP_BETA_PROBE_ID))).thenReturn(Lists.newArrayList(gcpBetaTarget));
        when(targetStore.getProbeTargets(Long.valueOf(GCP_COST_TARGET_ID))).thenReturn(Lists.newArrayList(gcpCostTarget));
        when(gcpBetaTarget.getId()).thenReturn(Long.valueOf(GCP_BETA_TARGET_ID));
        when(gcpCostTarget.getId()).thenReturn(Long.valueOf(GCP_COST_TARGET_ID));
        final MigrationProgressInfo migrationResult = migration.doStartMigration();
        assertEquals(MigrationStatus.SUCCEEDED, migrationResult.getStatus());
        // Verify that probes and targets for GCP Beta and GCP Cost have been removed from key value store.
        assertFalse(keyValueStore.containsKey(ProbeStore.PROBE_KV_STORE_PREFIX + GCP_BETA_PROBE_ID));
        assertFalse(keyValueStore.containsKey(ProbeStore.PROBE_KV_STORE_PREFIX + GCP_COST_PROBE_ID));
        assertFalse(keyValueStore.containsKey(TargetStore.TARGET_KV_STORE_PREFIX + GCP_BETA_TARGET_ID));
        assertFalse(keyValueStore.containsKey(TargetStore.TARGET_KV_STORE_PREFIX + GCP_COST_TARGET_ID));
    }
}
