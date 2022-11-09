package com.vmturbo.topology.processor.migration;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.MYSQL;

import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
        * This migration updates Mysql target info and probe info.
        * It adds/remove a new boolean flag `collectVmMetrics` that is responsible for
        * enabling/disabling the collection of virtual machines metrics.
        */
public class V_01_01_21__MysqlProbes_Remove_Vm_Metrics_Flag extends
        AbstractCollectVmMetricFlagDbMigration {

    private static final String PROPERTY_NAME = "collectVmMetrics";

    /**
     * Constructor.
     *
     * @param targetStore target store.
     * @param probeStore  probe store.
     */
    public V_01_01_21__MysqlProbes_Remove_Vm_Metrics_Flag(TargetStore targetStore,
                                                           ProbeStore probeStore) {
        super(targetStore,
                probeStore,
                MYSQL.getProbeType(),
                PROPERTY_NAME);
    }
}

