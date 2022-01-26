package com.vmturbo.components.common.setting;

/**
 * Enum definition for core socket ratio policy.
 */
public enum VcpuScalingSocketsCoresPerSocketModeEnum {
    /**
     * Discovered by probe.
     */
    PRESERVE_CORES_PER_SOCKET,

    /**
     * Specified by user in VcpuScaling_Sockets_CoresPerSocketValue setting.
     */
    USER_SPECIFIED
}
