package com.vmturbo.components.common.setting;

/**
 * Enum definition for core socket ratio policy.
 */
public enum CoreSocketRatioPolicyEnum {
    /**
     * Ignore the CSR sent from probe.
     */
    IGNORE,

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
