package com.vmturbo.topology.processor.cost;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 *
 */
public class CloudCostUtils {

    /**
     * Static mapping of Platform -> OSType
     */
    private static final Map<Platform, OSType> PLATFORM_OS_TYPE_MAP =
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
    public static final OSType platformToOSType(Platform platform) {
        return PLATFORM_OS_TYPE_MAP.getOrDefault(platform, OSType.UNKNOWN_OS);
    }

    // Prefixes used when generating local id's for storage entities in the cloud discovery probes
    public static final String AZURE_STORAGE_PREFIX = "azure::ST::";
    public static final String AWS_STORAGE_PREFIX = "aws::ST::";

    private static final Map<SDKProbeType, String> PROBE_TYPE_TO_STORAGE_PREFIX = ImmutableMap.of(
            SDKProbeType.AWS_COST, AWS_STORAGE_PREFIX,
            SDKProbeType.AWS, AWS_STORAGE_PREFIX,
            SDKProbeType.AWS_BILLING, AWS_STORAGE_PREFIX,
            SDKProbeType.AZURE, AZURE_STORAGE_PREFIX,
            SDKProbeType.AZURE_COST, AZURE_STORAGE_PREFIX
    );

    // prefixes used for database entities in the cloud discovery probes
    public static final String AZURE_DATABASE_TIER_PREFIX = "azure::DBPROFILE::";
    public static final String AWS_DATABASE_TIER_PREFIX = "aws::DBPROFILE::";
    private static final Map<SDKProbeType, String> PROBE_TYPE_TO_DATABASE_TIER_PREFIX = ImmutableMap.of(
            SDKProbeType.AWS_COST, AWS_DATABASE_TIER_PREFIX,
            SDKProbeType.AWS, AWS_DATABASE_TIER_PREFIX,
            SDKProbeType.AWS_BILLING, AWS_DATABASE_TIER_PREFIX,
            SDKProbeType.AZURE, AZURE_DATABASE_TIER_PREFIX,
            SDKProbeType.AZURE_COST, AZURE_DATABASE_TIER_PREFIX
    );

    /**
     * Converts a storage tier's local name to a probe-type-based string id. Ideally these id's
     * would come straight from the probes. But since we don't want to change the probes right now,
     * we are going to use this conversion function until we can either add a probe wrapper for
     * the billing and cost probes, or update the probes themselves.
     *
     * Examples:
     * <ul>
     *     <li>localName <em>"sc1"</em> for an AWS Probe would become <em>"aws::ST::sc1"</em></li>
     *     <li>localName <em>"sc1"</em> for an Azure Probe would become <em>"azure::ST::sc1"</em></li>
     * </ul>
     *
     * Note that this function is not used in the cloud discovery wrapper probes, and we don't
     * necessarily want it to be. So it will need to be updated separately. All the more reason to
     * move this stuff into the probes or wrappers.
     *
     * @param localName the local id of the storage tier
     * @param probeType the {@link SDKProbeType} of the probe that discovered this entity
     * @return a probe-based identifier for the storage tier
     */
    public static String storageTierLocalNameToId(@Nonnull String localName, @Nonnull SDKProbeType probeType) {
        if (PROBE_TYPE_TO_STORAGE_PREFIX.containsKey(probeType)) {
            return PROBE_TYPE_TO_STORAGE_PREFIX.get(probeType) + localName.toUpperCase();
        }
        // if a probe type is not found in the map, then return the original string unaltered.
        return localName;
    }

    /**
     * Converts a database tier's local name to a probe-type-based string id. Ideally these id's
     * would come straight from the probes. But since we don't want to change the probes right now,
     * we are going to use this conversion function until we can either add a probe wrapper for
     * the billing and cost probes, or update the probes themselves.
     *
     * Examples:
     * <ul>
     *     <li>localName <em>"db1"</em> for an AWS Probe would become <em>"aws::DBPROFILE::db1"</em></li>
     *     <li>localName <em>"db1"</em> for an Azure Probe would become <em>"azure::DBPROFILE::db1"</em></li>
     * </ul>
     *
     * Note that this function is not used in the cloud discovery wrapper probes, and we don't
     * necessarily want it to be.
     *
     * @param localName the local id of the database tier
     * @param probeType the {@link SDKProbeType} of the probe that discovered this entity
     */
    public static String databaseTierLocalNameToId(@Nonnull String localName, @Nonnull SDKProbeType probeType) {
        if (PROBE_TYPE_TO_DATABASE_TIER_PREFIX.containsKey(probeType)) {
            return PROBE_TYPE_TO_DATABASE_TIER_PREFIX.get(probeType) + localName;
        }
        // if a probe type is not found in the map, then return the original string unaltered.
        return localName;
    }
}
