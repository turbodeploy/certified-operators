package com.vmturbo.topology.processor.cost;

import java.util.Map;
import java.util.function.BiFunction;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData.Platform;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadCloudCostDataStage;

/**
 * Helper class to match DB and DBS tiers to their respective pricing data as part of
 * {@link UploadCloudCostDataStage} stage.
 */
public class CloudCostUtils {

    /**
     * Static mapping of Platform -> OSType.
     */
    private static final Map<Platform, OSType> PLATFORM_OS_TYPE_MAP =
            ImmutableMap.<Platform, OSType>builder()
                    .put(Platform.LINUX, OSType.LINUX)
                    .put(Platform.SUSE, OSType.SUSE)
                    .put(Platform.RHEL, OSType.RHEL)
                    .put(Platform.WINDOWS, OSType.WINDOWS)
                    .put(Platform.WINDOWS_WITH_SQL_STANDARD, OSType.WINDOWS_WITH_SQL_STANDARD)
                    .put(Platform.WINDOWS_WITH_SQL_WEB, OSType.WINDOWS_WITH_SQL_WEB)
                    .put(Platform.WINDOWS_WITH_SQL_SERVER_ENTERPRISE, OSType.WINDOWS_WITH_SQL_ENTERPRISE)
                    .build();

    /**
     * Utility function for converting from {@link Platform} to {@link OSType} enum values.
     * Unfortunately, because the protobuf number and enum value names are both different, this is
     * a static mapping from one to the other. It may be brittle if we expect the list to change.
     *
     * @param platform The operating system used; e.g. Linux.
     * @return The supported operating systems. Keep in sync with com.vmturbo.mediation.hybrid.cloud.common.OsType.
     */
    public static OSType platformToOSType(Platform platform) {
        return PLATFORM_OS_TYPE_MAP.getOrDefault(platform, OSType.UNKNOWN_OS);
    }

    private static final Map<SDKProbeType, BiFunction<TopologyStitchingEntity, SDKProbeType, String>>
            DB_TIER_LOCAL_NAME_TO_ID_FUNCTION = ImmutableMap.of(
            SDKProbeType.AZURE, (topologyStitchingEntity, probeType) -> azureDatabaseTierLocalNameToId(topologyStitchingEntity, probeType),
            SDKProbeType.AWS, (topologyStitchingEntity, probeType) -> awsDatabaseTierEntityToId(topologyStitchingEntity, probeType),
            SDKProbeType.AWS_COST, (topologyStitchingEntity, probeType) -> awsDatabaseTierEntityToId(topologyStitchingEntity, probeType),
            SDKProbeType.AWS_BILLING, (topologyStitchingEntity, probeType) -> awsDatabaseTierEntityToId(topologyStitchingEntity, probeType)
    );

    /**
     * Looks up  and returns a function based on {@link SDKProbeType}
     * which is used for formatting DB Tier localId.
     */
    private static final Map<SDKProbeType, BiFunction<TopologyStitchingEntity, SDKProbeType, String>>
            DB_TIER_FULL_NAME_TO_ID_FUNCTION = ImmutableMap.of(
            SDKProbeType.AZURE, (topologyStitchingEntity, probeType) -> azureDatabaseTierFullNameToId(topologyStitchingEntity, probeType),
            SDKProbeType.AZURE_COST, (topologyStitchingEntity, probeType) -> azureDatabaseTierFullNameToId(topologyStitchingEntity, probeType),
            SDKProbeType.AWS, (topologyStitchingEntity, probeType) -> awsDatabaseTierEntityToId(topologyStitchingEntity, probeType),
            SDKProbeType.AWS_COST, (topologyStitchingEntity, probeType) -> awsDatabaseTierEntityToId(topologyStitchingEntity, probeType)
            );

    // prefixes used for database entities in the cloud discovery probes
    private static final String AZURE_DATABASE_TIER_PREFIX = "azure::DBPROFILE::";
    private static final String AWS_DATABASE_SERVER_TIER_PREFIX = "aws::DBSPROFILE::";
    private static final String AZURE_COMPUTE_STORAGE_DELIMITER = "/";
    // Prefixes used when generating local id's for entities in the cloud discovery probes
    private static final String AZURE_STORAGE_PREFIX = "azure::ST::";
    private static final String AWS_STORAGE_PREFIX = "aws::ST::";
    private static final String AZURE_STANDARD_DATABASE_PREFIX = "Standard_";
    private static final String AZURE_PREMIUM_DATABASE_PREFIX = "Premium_";
    private static final String EMPTY_PREFIX = "";

    private static final Map<SDKProbeType, String> PROBE_TYPE_TO_DATABASE_TIER_PREFIX = ImmutableMap.of(
            SDKProbeType.AWS_COST, AWS_DATABASE_SERVER_TIER_PREFIX,
            SDKProbeType.AWS, AWS_DATABASE_SERVER_TIER_PREFIX,
            SDKProbeType.AWS_BILLING, AWS_DATABASE_SERVER_TIER_PREFIX,
            SDKProbeType.AZURE, AZURE_DATABASE_TIER_PREFIX,
            SDKProbeType.AZURE_COST, AZURE_DATABASE_TIER_PREFIX
    );
    private static final Map<SDKProbeType, String> PROBE_TYPE_TO_STORAGE_PREFIX = ImmutableMap.of(
            SDKProbeType.AWS_COST, AWS_STORAGE_PREFIX,
            SDKProbeType.AWS, AWS_STORAGE_PREFIX,
            SDKProbeType.AWS_BILLING, AWS_STORAGE_PREFIX,
            SDKProbeType.AZURE, AZURE_STORAGE_PREFIX,
            SDKProbeType.AZURE_COST, AZURE_STORAGE_PREFIX
    );

    // Map for matching the Azure SDK Probe letter to Azure cost probe prefix
    private static final Map<String, String> AZURE_DATABASE_LETTER_TO_NAME = ImmutableMap.of(
            "S", AZURE_STANDARD_DATABASE_PREFIX,
            "P", AZURE_PREMIUM_DATABASE_PREFIX,
            "F", EMPTY_PREFIX,
            "B", EMPTY_PREFIX
    );

    /**
     * Private constructor.
     */
    private CloudCostUtils() {
    }

    /**
     * Converts a storage tier's local name to a probe-type-based string id. Ideally these id's
     * would come straight from the probes. But since we don't want to change the probes right now,
     * we are going to use this conversion function until we can either add a probe wrapper for
     * the billing and cost probes, or update the probes themselves.
     * <p>
     * Examples:
     * <ul>
     * <li>localName <em>"sc1"</em> for an AWS Probe would become <em>"aws::ST::sc1"</em></li>
     * <li>localName <em>"sc1"</em> for an Azure Probe would become <em>"azure::ST::sc1"</em></li>
     * </ul>
     * <p>
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
     * <p>
     * Examples:
     * <ul>
     * <li>localName <em>"db1"</em> for an AWS Probe would become <em>"aws::DBSPROFILE::db1"</em></li>
     * <li>localName <em>"db1"</em> for an Azure Probe would become <em>"azure::DBPROFILE::db1"</em></li>
     * </ul>
     * <p>
     * Note that this function is not used in the cloud discovery wrapper probes, and we don't
     * necessarily want it to be.
     *  @param topologyStitchingEntity the local id of the database tier
     * @param probeType the {@link SDKProbeType} of the probe that discovered this entity
     * @return databaseTier name.
     */
    public static String getEntityIdFromDBProfile(@Nonnull TopologyStitchingEntity topologyStitchingEntity, @Nonnull SDKProbeType probeType) {
        if (PROBE_TYPE_TO_DATABASE_TIER_PREFIX.containsKey(probeType)) {
            return DB_TIER_LOCAL_NAME_TO_ID_FUNCTION.get(probeType).apply(topologyStitchingEntity, probeType);
        }
        // if a probe type is not found in the map, then return the original string unaltered.
        return topologyStitchingEntity.getDisplayName();
    }

    /**
     * There are some situation in Azure cost v Azure Subscription where there are multiple DB Tier
     * for the same DB Tier cost. and hence record only one DB Tier cost.
     * Eg: Premium_P11 :DB cost entry but both
     * Premium P11 / 1048576.0 MegaBytes and Premium P11 / 4194304.0 MegaBytes matches same DB cost entry.
     * Input eg: dbName : "P11 / 4194304.0 MegaBytes", probeType: AZURE
     * Output: "azure::DBPROFILE::Premium_P11 / 4194304.0 MegaBytes".
     *
     * @param entity    the {@link TopologyStitchingEntity} of the database(server) tier.
     * @param probeType the {@link SDKProbeType} of the probe that discovered this entity.
     * @return string with either an updated ID (if probe type is found) or return original string.
     */
    public static String getEntityFullNameFromDBProfile(@Nonnull TopologyStitchingEntity entity, @Nonnull SDKProbeType probeType) {
        if (PROBE_TYPE_TO_DATABASE_TIER_PREFIX.containsKey(probeType)) {
            BiFunction<TopologyStitchingEntity, SDKProbeType, String> biFunction = DB_TIER_FULL_NAME_TO_ID_FUNCTION.get(probeType);
            if (biFunction != null) {
                return biFunction.apply(entity, probeType);
            }
        }
        // if a probe type is not found in the map, then return the original string unaltered.
        return entity.getDisplayName() + entity.getLocalId();
    }

    /**
     * Helper function to construct DB tier id in order to match the database tiers
     * coming from regular Azure probe and the db costs coming from azure cost probe.
     * e.g. "S6 / 2048.0 MegaBytes" -> "azure::DBPROFILE::Standard_S6"
     *
     * @param entity {@link TopologyStitchingEntity} the database tier.
     * @param probeType the {@link SDKProbeType} of the probe that discovered this entity.
     * @return the string Azure db constructed id.
     */
    private static String azureDatabaseTierLocalNameToId(@Nonnull TopologyStitchingEntity entity,
                                                         @Nonnull SDKProbeType probeType) {
        final String localName = entity.getDisplayName();
        int indexOfStorageDelimiter = entity.getDisplayName().indexOf(AZURE_COMPUTE_STORAGE_DELIMITER);
        String dbTypeName = indexOfStorageDelimiter == -1 ?
                localName : localName.substring(0, indexOfStorageDelimiter).trim();
        String dbTypeLetter = dbTypeName.substring(0, 1);
        return PROBE_TYPE_TO_DATABASE_TIER_PREFIX.get(probeType) +
                AZURE_DATABASE_LETTER_TO_NAME.get(dbTypeLetter) + dbTypeName;
    }

    /**
     * Returns formatted name for a given name and probeType.
     * eg: Input eg: localName : "P11 / 4194304.0 MegaBytes", probeType : AZURE
     * Output: "azure::DBPROFILE::Premium_P11 / 4194304.0 MegaBytes".
     * @param entity {@link TopologyStitchingEntity} azure DB name.
     * @param probeType {@link SDKProbeType} to which the DB belongs to.
     * @return formatted string.
     */
    private static String azureDatabaseTierFullNameToId(@Nonnull TopologyStitchingEntity entity,
                                                       @Nonnull SDKProbeType probeType) {
        final String fullName = entity.getDisplayName() + entity.getLocalId();
        String dbTypeLetter = fullName.substring(0, 1);
        return PROBE_TYPE_TO_DATABASE_TIER_PREFIX.get(probeType) +
                AZURE_DATABASE_LETTER_TO_NAME.get(dbTypeLetter) + fullName;
    }

    /**
     * Helper function to construct DB tier id in order to match the database tiers
     * coming from regular AWS probe and the db costs coming from azure cost probe.
     *
     * @param entity {@link TopologyStitchingEntity} for database tier entity.
     * @param probeType the {@link SDKProbeType} of the probe that discovered this entity.
     * @return the string AWS db constructed id.
     */
    private static String awsDatabaseTierEntityToId(@Nonnull TopologyStitchingEntity entity,
                                                    @Nonnull SDKProbeType probeType) {
        final String identifier;
        final Builder entityBuilder = entity.getEntityBuilder();
        if (entityBuilder.hasDatabaseServerTierData()
                && entityBuilder.getDatabaseServerTierData().hasTemplateIdentifier()) {
            identifier = entityBuilder.getDatabaseServerTierData().getTemplateIdentifier();
        } else {
            identifier = entity.getDisplayName();
        }
        return PROBE_TYPE_TO_DATABASE_TIER_PREFIX.get(probeType) + identifier;
    }
}
