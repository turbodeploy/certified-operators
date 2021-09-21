package com.vmturbo.topology.processor.migration;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.APPDYNAMICS;

import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This migration updates AppDynamics target info and probe info.
 * It adds a new boolean flag `collectVmMetrics` that is responsible for
 * enabling/disabling the collection of virtual machines metrics.
 */
public class V_01_01_11__AppDynamics_Add_Vm_Metrics_Flag extends
                AbstractCollectVmMetricFlagMigration {

    /**
     * Constructor.
     *
     * @param targetStore target store.
     * @param probeStore  probe store.
     */
    public V_01_01_11__AppDynamics_Add_Vm_Metrics_Flag(TargetStore targetStore,
                                                       ProbeStore probeStore) {
        super(targetStore, probeStore, APPDYNAMICS.getProbeType());
    }
}
