package com.vmturbo.components.common.setting;

/**
 * Enum definition for core socket ratio policy.
 */
public enum VcpuScalingSocketsCoresPerSocketModeEnum {
    /**
     * Discovered by probe.
     */
    PRESERVE,

    /**
     * Specified by user in VcpuScaling_Sockets_CoresPerSocketValue setting.
     */
    USER_SPECIFIED
}
