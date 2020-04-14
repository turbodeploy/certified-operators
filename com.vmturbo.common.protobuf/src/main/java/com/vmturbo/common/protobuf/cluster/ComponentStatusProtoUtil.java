package com.vmturbo.common.protobuf.cluster;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;

/**
 * Utilities for component notifications.
 */
public class ComponentStatusProtoUtil {

    private ComponentStatusProtoUtil() { }

    /**
     * Utility class to log the details in a {@link ComponentIdentifier}.
     *
     * @param componentId The identifier.
     * @return String to use for logs.
     */
    public static String getComponentLogId(@Nonnull final ComponentIdentifier componentId) {
        return "(type : " + componentId.getComponentType() + ", id : " +
            componentId.getInstanceId() + ", jvm : " + componentId.getJvmId() + ")";
    }
}
