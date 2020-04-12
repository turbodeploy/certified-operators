package com.vmturbo.mediation.gcp;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.conversion.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.cloud.converter.ApplicationComponentConverter;
import com.vmturbo.mediation.conversion.cloud.converter.BusinessAccountConverter;
import com.vmturbo.mediation.conversion.cloud.converter.ComputeTierConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DatabaseConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DatabaseServerConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DatabaseServerTierConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DiskArrayConverter;
import com.vmturbo.mediation.conversion.cloud.converter.LoadBalancerConverter;
import com.vmturbo.mediation.conversion.cloud.converter.ServiceConverter;
import com.vmturbo.mediation.conversion.cloud.converter.StorageConverter;
import com.vmturbo.mediation.conversion.cloud.converter.VirtualMachineConverter;
import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.mediation.conversion.util.ConverterUtils;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The conversion context provided by GCP probe which contains logic specific to GCP.
 */
public class GcpConversionContext implements CloudProviderConversionContext {

    private final Logger logger = LogManager.getLogger();

    // converters for different entity types
    private static final Map<EntityType, IEntityConverter> GCP_ENTITY_CONVERTERS;
    static {
        final Map<EntityType, IEntityConverter> converters = new EnumMap<>(EntityType.class);
        converters.put(EntityType.VIRTUAL_MACHINE, new VirtualMachineConverter(SDKProbeType.GCP));
        converters.put(EntityType.COMPUTE_TIER, new ComputeTierConverter(SDKProbeType.GCP));
        converters.put(EntityType.DATABASE, new DatabaseConverter(SDKProbeType.GCP));
        converters.put(EntityType.BUSINESS_ACCOUNT, new BusinessAccountConverter(SDKProbeType.GCP));
        converters.put(EntityType.STORAGE, new StorageConverter(SDKProbeType.GCP));
        converters.put(EntityType.DATABASE_SERVER_TIER, new DatabaseServerTierConverter());
        converters.put(EntityType.DATABASE_SERVER, new DatabaseServerConverter(SDKProbeType.GCP));
        converters.put(EntityType.LOAD_BALANCER, new LoadBalancerConverter());
        converters.put(EntityType.APPLICATION_COMPONENT, new ApplicationComponentConverter());
        converters.put(EntityType.SERVICE, new ServiceConverter());
        converters.put(EntityType.DISK_ARRAY, new DiskArrayConverter());
        GCP_ENTITY_CONVERTERS = Collections.unmodifiableMap(converters);
    }

    // map showing which EntityType to be owned by which CloudService
    private static final Map<EntityType, CloudService> ENTITY_TYPE_OWNED_BY_CLOUD_SERVICE_MAP =
            ImmutableMap.of(
                    EntityType.COMPUTE_TIER, CloudService.GCP_VIRTUAL_MACHINES,
                    EntityType.STORAGE_TIER, CloudService.GCP_STORAGE
            );

    private static final Map<EntityType, EntityType> GCP_PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE =
            ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE, EntityType.COMPUTE_TIER,
                    EntityType.DATABASE_SERVER, EntityType.DATABASE_SERVER_TIER
            );

    @Nonnull
    @Override
    public Map<EntityType, IEntityConverter> getEntityConverters() {
        return GCP_ENTITY_CONVERTERS;
    }

    @Nonnull
    @Override
    public String getStorageTierId(@Nonnull String storageTierName) {
        return "gcp::ST::" + storageTierName;
    }

    /**
     * Get region id based on AZ id. It gets "ca-central-1" from AZ id and then combine
     * with prefix into the Region id. For example:
     *     GCP AZ id is:     GCP::ca-central-1::PM::ca-central-1b
     *     GCP Region id is: GCP::ca-central-1::DC::ca-central-1b
     *
     * @param azId id of the AZ
     * @return region id for the AZ
     */
    @Nonnull
    @Override
    public String getRegionIdFromAzId(@Nonnull String azId) {
        String region = azId.split("::", 3)[1];
        return "gcp::" + region + "::DC::" + region;
    }

    /**
     * Get AZ id based on region id. It gets "ca-central-1" from region id and then combine
     * with prefix into the AZ id. For example:
     *     AWS AZ id is:     aws::ca-central-1::PM::ca-central-1b
     *     AWS Region id is: aws::ca-central-1::DC::ca-central-1b
     *
     * @param regionId id of the region
     * @return id of the AZ
     */
    @Nonnull
    @Override
    public String getAzIdFromRegionId(@Nonnull String regionId) {
        String region = regionId.split("::", 3)[1];
        return "gcp::" + region + "::PM::" + region;
    }

    @Nonnull
    @Override
    public Optional<String> getVolumeIdFromStorageFilePath(@Nullable String regionName,
                                                           @Nonnull String filePath) {
        if (regionName == null) {
            logger.error("Null region name for file {}", filePath);
            return Optional.empty();
        }
        return Optional.of("gcp::" + regionName + "::VL::" + filePath);
    }

    @Nonnull
    @Override
    public Optional<CloudService> getCloudServiceOwner(@Nonnull EntityType entityType) {
        return Optional.ofNullable(ENTITY_TYPE_OWNED_BY_CLOUD_SERVICE_MAP.get(entityType));
    }

    @Nonnull
    @Override
    public Set<CloudService> getCloudServicesToCreate() {
        return ConverterUtils.getCloudServicesByProbeType(SDKProbeType.GCP);
    }

    @Nonnull
    @Override
    public Optional<EntityType> getCloudEntityTypeForProfileType(@Nonnull EntityType entityType) {
        return Optional.ofNullable(GCP_PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE.get(entityType));
    }
}
