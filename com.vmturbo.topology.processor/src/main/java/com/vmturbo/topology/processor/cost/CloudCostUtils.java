package com.vmturbo.topology.processor.cost;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 *
 */
public class CloudCostUtils {

    /**
     * Static mapping of Platform -> OSType
     */
    static private final Map<Platform, OSType> PLATFORM_OS_TYPE_MAP =
            ImmutableMap.<Platform,OSType>builder()
                .put(Platform.LINUX, OSType.LINUX)
                .put(Platform.SUSE, OSType.SUSE)
                .put(Platform.RHEL, OSType.RHEL)
                .put(Platform.WINDOWS, OSType.WINDOWS)
                .put(Platform.WINDOWS_SQL_STANDARD, OSType.WINDOWS_WITH_SQL_STANDARD)
                .put(Platform.WINDOWS_SQL_WEB, OSType.WINDOWS_WITH_SQL_WEB)
                .put(Platform.WINDOWS_SQL_SERVER_ENTERPRISE, OSType.WINDOWS_WITH_SQL_ENTERPRISE)
                .build();

    /**
     *
     * Utility function for converting from {@link Platform} to {@link OSType} enum values.
     * Unfortunately, because the protobuf number and enum value names are both different, this is
     * a static mapping from one to the other. It may be brittle if we expect the list to change.
     *
     * @param platform
     * @return
     */
    static public OSType platformToOSType(Platform platform) {
        return PLATFORM_OS_TYPE_MAP.getOrDefault(platform, OSType.UNKNOWN_OS);
    }


}
