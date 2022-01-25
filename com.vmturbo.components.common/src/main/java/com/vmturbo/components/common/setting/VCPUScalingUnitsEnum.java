package com.vmturbo.components.common.setting;

/**
 * Enum definition for core socket ratio policy.
 */
public enum VCPUScalingUnitsEnum {
    /**
     * Ignores the CSR sent from probe.
     */
    MHZ,

    /**
     * Use the CSR sent from probe.
     */
    SOCKETS,

    /**
     * Prefer cores or sockets in scaling.
     */
    CORES,

    /**
     * Force csr to 1 and scale in sockets. specified by VcpuScaling_Vcpus_VcpusIncrementValue.
     */
    VCPUS
}
