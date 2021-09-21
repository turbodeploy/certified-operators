package com.vmturbo.topology.processor.migration;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This migration updates Dynatrace target info and probe info.
 * It adds a new boolean flag `collectVmMetrics` that is responsible for
 * enabling/disabling the collection of virtual machines metrics.
 */
public class V_01_01_10__Dynatrace_Add_Vm_Metrics_Flag extends
                AbstractCollectVmMetricFlagMigration {

    /**
     * Constructor.
     *
     * @param targetStore target store.
     * @param probeStore  probe store.
     */
    public V_01_01_10__Dynatrace_Add_Vm_Metrics_Flag(TargetStore targetStore,
                                                     ProbeStore probeStore) {
        super(targetStore, probeStore, SDKProbeType.DYNATRACE.getProbeType());
    }
}
