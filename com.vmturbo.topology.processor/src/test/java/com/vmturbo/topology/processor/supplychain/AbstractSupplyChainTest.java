package com.vmturbo.topology.processor.supplychain;

import java.util.Optional;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.junit.Before;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Abstract class that creating common fields and method for supply chain validation test.
 */
public class AbstractSupplyChainTest {

    public static final long HYPERVISOR_TARGET_ID = 2333L;
    public static final long HYPERVISOR_TARGET_1_ID = 2333009L;
    public static final long STORAGE_TARGET_ID = 6666L;
    public static final long BROKEN_TARGET_ID = 129921L;
    public static final long COMPETING_TARGET_ID = 170002L;

    public static final long HYPERVISOR_PROBE_ID = 12345L;
    public static final long STORAGE_PROBE_ID = 54321L;
    public static final long BROKEN_PROBE_ID = 99762L;
    public static final long COMPETING_PROBE_ID = 81900L;

    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);

    private final Target hypervisorTarget = Mockito.mock(Target.class);
    private final Target storageTarget = Mockito.mock(Target.class);
    private final Target brokenTarget = Mockito.mock(Target.class);
    private final Target competingTarget = Mockito.mock(Target.class);
    private final Target hypervisorTarget1 = Mockito.mock(Target.class);

    private final ProbeInfo.Builder hypervisorProbeBuilder = ProbeInfo.newBuilder(Probes.emptyProbe)
            .addAllSupplyChainDefinitionSet(SupplyChainTestUtils.hypervisorSupplyChain());
    private final ProbeInfo.Builder storageProbeBuilder = ProbeInfo.newBuilder(Probes.emptyProbe)
            .addAllSupplyChainDefinitionSet(SupplyChainTestUtils.storageSupplyChain());
    private final ProbeInfo.Builder brokenProbeBuilder = ProbeInfo.newBuilder(Probes.emptyProbe)
            .addAllSupplyChainDefinitionSet(SupplyChainTestUtils.brokenSupplyChain());
    private final ProbeInfo.Builder competingProbeBuilder = ProbeInfo.newBuilder(Probes.emptyProbe)
            .addAllSupplyChainDefinitionSet(SupplyChainTestUtils.competingSupplyChain());

    public ProbeStore getProbeStore() {
        return probeStore;
    }

    public TargetStore getTargetStore() {
        return targetStore;
    }

    @Before
    public void init() {
        Mockito.when(hypervisorTarget.getProbeId()).thenReturn(HYPERVISOR_PROBE_ID);
        Mockito.when(storageTarget.getProbeId()).thenReturn(STORAGE_PROBE_ID);
        Mockito.when(brokenTarget.getProbeId()).thenReturn(BROKEN_PROBE_ID);
        Mockito.when(competingTarget.getProbeId()).thenReturn(COMPETING_PROBE_ID);
        Mockito.when(hypervisorTarget1.getProbeId()).thenReturn(HYPERVISOR_PROBE_ID);

        Mockito.when(targetStore.getTarget(Matchers.eq(HYPERVISOR_TARGET_ID))).
            thenReturn(Optional.of(hypervisorTarget));
        Mockito.when(targetStore.getTarget(Matchers.eq(STORAGE_TARGET_ID))).
            thenReturn(Optional.of(storageTarget));
        Mockito.when(targetStore.getTarget(Matchers.eq(BROKEN_TARGET_ID))).
            thenReturn(Optional.of(brokenTarget));
        Mockito.when(targetStore.getTarget(Matchers.eq(COMPETING_TARGET_ID))).
            thenReturn(Optional.of(competingTarget));
        Mockito.when(targetStore.getTarget(Matchers.eq(HYPERVISOR_TARGET_1_ID))).
            thenReturn(Optional.of(hypervisorTarget1));

        Mockito.when(probeStore.getProbe(Matchers.eq(HYPERVISOR_PROBE_ID))).
            thenReturn(Optional.of(hypervisorProbeBuilder.build()));
        Mockito.when(probeStore.getProbe(Matchers.eq(STORAGE_PROBE_ID))).
            thenReturn(Optional.of(storageProbeBuilder.build()));
        Mockito.when(probeStore.getProbe(Matchers.eq(BROKEN_PROBE_ID))).
            thenReturn(Optional.of(brokenProbeBuilder.build()));
        Mockito.when(probeStore.getProbe(Matchers.eq(COMPETING_PROBE_ID))).
            thenReturn(Optional.of(competingProbeBuilder.build()));
    }
}
