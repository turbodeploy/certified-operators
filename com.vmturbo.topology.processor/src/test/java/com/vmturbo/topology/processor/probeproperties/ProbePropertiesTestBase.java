package com.vmturbo.topology.processor.probeproperties;

import java.util.Map;
import java.util.Optional;

import org.mockito.Mockito;
import org.mockito.Spy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Definitions common to tests that have to do with probe properties.
 */
public class ProbePropertiesTestBase {
    // construct probe property store
    protected final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    protected final TargetStore targetStore = Mockito.mock(TargetStore.class);
    protected final KeyValueStore keyValueStore = new MapKeyValueStore();
    protected final KVBackedProbePropertyStore
        probePropertyStore = new KVBackedProbePropertyStore(probeStore, targetStore, keyValueStore);

    @Spy
    protected final RemoteMediationServer mediationServer = Mockito.mock(RemoteMediationServer.class);

    // declare probes and targets
    private final ProbeInfo probe1 = ProbeInfo.newBuilder().setProbeType("1").setProbeCategory("").setUiProbeCategory("").build();
    private final ProbeInfo probe2 = ProbeInfo.newBuilder().setProbeType("2").setProbeCategory("").setUiProbeCategory("").build();
    private final Target target11 = Mockito.mock(Target.class);
    private final Target target12 = Mockito.mock(Target.class);
    private final Target target2 = Mockito.mock(Target.class);

    // ids
    protected static final long PROBE_ID_1 = 1L;
    protected static final long PROBE_ID_2 = 2L;
    protected static final long NON_EXISTENT_PROBE_ID = 3L;
    protected static final long TARGET_ID_11 = 11L;
    protected static final long TARGET_ID_12 = 12L;
    protected static final long TARGET_ID_2 = 2L;
    protected static final String TARGET_ADD_11 = "ADDRESS11";
    protected static final String TARGET_ADD_12 = "ADDRESS12";
    protected static final String TARGET_ADD_2 = "ADDRESS2";
    protected static final long NON_EXISTENT_TARGET_ID = 3L;

    // property maps
    protected static final Map<String, String>
        PROBE_PROPERTY_MAP_1 = ImmutableMap.of("A", "Avalue", "B", "Bvalue");
    protected static final Map<String, String>
        PROBE_PROPERTY_MAP_2 = ImmutableMap.of("A", "Avalue1", "C", "Cvalue");

    /**
     * Set up mock probes and targets.
     */
    public void setUp() {
        // probes
        Mockito.when(probeStore.getProbe(PROBE_ID_1)).thenReturn(Optional.of(probe1));
        Mockito.when(probeStore.getProbe(PROBE_ID_2)).thenReturn(Optional.of(probe2));
        Mockito.when(probeStore.getProbe(NON_EXISTENT_PROBE_ID)).thenReturn(Optional.empty());
        Mockito
            .when(probeStore.getProbes())
            .thenReturn(ImmutableMap.of(PROBE_ID_1, probe1, PROBE_ID_2, probe2));

        // targets
        Mockito.when(targetStore.getTarget(TARGET_ID_11)).thenReturn(Optional.of(target11));
        Mockito.when(targetStore.getTarget(TARGET_ID_12)).thenReturn(Optional.of(target12));
        Mockito.when(targetStore.getTarget(TARGET_ID_2)).thenReturn(Optional.of(target2));
        Mockito.when(targetStore.getTarget(NON_EXISTENT_TARGET_ID)).thenReturn(Optional.empty());
        Mockito.when(target11.getProbeId()).thenReturn(PROBE_ID_1);
        Mockito.when(target12.getProbeId()).thenReturn(PROBE_ID_1);
        Mockito.when(target2.getProbeId()).thenReturn(PROBE_ID_2);
        Mockito.when(target11.getProbeId()).thenReturn(PROBE_ID_1);
        Mockito.when(target11.getId()).thenReturn(TARGET_ID_11);
        Mockito.when(target12.getId()).thenReturn(TARGET_ID_12);
        Mockito.when(target2.getId()).thenReturn(TARGET_ID_2);
        Mockito.when(targetStore.getAll()).thenReturn(ImmutableList.of(target11, target12, target2));
        Mockito
            .when(targetStore.getProbeTargets(PROBE_ID_1))
            .thenReturn(ImmutableList.of(target11, target12));
        Mockito.when(targetStore.getProbeTargets(PROBE_ID_2)).thenReturn(ImmutableList.of(target2));
        Mockito.when(target11.getDisplayName()).thenReturn(TARGET_ADD_11);
        Mockito.when(target12.getDisplayName()).thenReturn(TARGET_ADD_12);
        Mockito.when(target2.getDisplayName()).thenReturn(TARGET_ADD_2);
    }
}
