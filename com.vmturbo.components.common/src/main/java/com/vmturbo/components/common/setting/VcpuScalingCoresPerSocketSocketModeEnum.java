package com.vmturbo.components.common.setting;

/**
 * Enum definition for core socket ratio policy.
 */
public enum VcpuScalingCoresPerSocketSocketModeEnum {
    /**
     * Discovered by probe.
     */
    PRESERVE,

    /**
     * Specified by user in VcpuScaling_CoresPerSocket_SocketValue setting.
     */
    USER_SPECIFIED,

    /**
     * The sockets number meets what discovered from the host.
     */
    MATCH_HOST,
}
