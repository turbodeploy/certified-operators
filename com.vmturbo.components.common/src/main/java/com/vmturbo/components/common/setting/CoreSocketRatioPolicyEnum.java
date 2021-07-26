package com.vmturbo.components.common.setting;

/**
 * Enum definition for core socket ratio policy.
 */
public enum CoreSocketRatioPolicyEnum {
    /**
     * Ignores the CSR sent from probe.
     */
    LEGACY,

    /**
     * Use the CSR sent from probe.
     */
    RESPECT,

    /**
     * Prefer cores or sockets in scaling.
     */
    CONTROL,
    /**
     * User specify the CSR.
     */
    SPECIFY
}
