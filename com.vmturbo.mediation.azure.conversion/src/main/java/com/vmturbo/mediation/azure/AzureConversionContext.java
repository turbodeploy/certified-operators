package com.vmturbo.mediation.azure;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.conversion.cloud.CloudProviderConversionContext;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.cloud.converter.ApplicationConverter;
import com.vmturbo.mediation.conversion.cloud.converter.AvailabilityZoneConverter;
import com.vmturbo.mediation.conversion.cloud.converter.BusinessAccountConverter;
import com.vmturbo.mediation.conversion.cloud.converter.ComputeTierConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DatabaseConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DatabaseServerConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DatabaseTierConverter;
import com.vmturbo.mediation.conversion.cloud.converter.DiskArrayConverter;
import com.vmturbo.mediation.conversion.cloud.converter.LoadBalancerConverter;
import com.vmturbo.mediation.conversion.cloud.converter.RegionConverter;
import com.vmturbo.mediation.conversion.cloud.converter.VirtualApplicationConverter;
import com.vmturbo.mediation.conversion.cloud.converter.VirtualMachineConverter;
import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * The conversion context provided by Azure probe which contains logic specific to Azure.
 */
public class AzureConversionContext implements CloudProviderConversionContext {

    // converters for different entity types
    private static final Map<EntityType, IEntityConverter> AZURE_ENTITY_CONVERTERS;
    static {
        final Map<EntityType, IEntityConverter> converters = new EnumMap<>(EntityType.class);
        converters.put(EntityType.VIRTUAL_MACHINE, new VirtualMachineConverter(SDKProbeType.AZURE));
        converters.put(EntityType.AVAILABILITY_ZONE, new AvailabilityZoneConverter(SDKProbeType.AZURE));
        converters.put(EntityType.COMPUTE_TIER, new ComputeTierConverter(SDKProbeType.AZURE));
        converters.put(EntityType.DATABASE, new DatabaseConverter(SDKProbeType.AZURE));
        converters.put(EntityType.BUSINESS_ACCOUNT, new BusinessAccountConverter(SDKProbeType.AZURE));
        converters.put(EntityType.REGION, new RegionConverter());
        converters.put(EntityType.STORAGE, new AzureStorageConverter());
        converters.put(EntityType.DATABASE_TIER, new DatabaseTierConverter());
        converters.put(EntityType.DATABASE_SERVER, new DatabaseServerConverter(SDKProbeType.AZURE));
        converters.put(EntityType.LOAD_BALANCER, new LoadBalancerConverter());
        converters.put(EntityType.APPLICATION, new ApplicationConverter());
        converters.put(EntityType.VIRTUAL_APPLICATION, new VirtualApplicationConverter());
        converters.put(EntityType.DISK_ARRAY, new DiskArrayConverter());
        AZURE_ENTITY_CONVERTERS = Collections.unmodifiableMap(converters);
    }

    // map showing which EntityType to be owned by which CloudService
    private static final Map<EntityType, CloudService> ENTITY_TYPE_OWNED_BY_CLOUD_SERVICE_MAP =
            ImmutableMap.of(
                    EntityType.COMPUTE_TIER, CloudService.AZURE_VIRTUAL_MACHINES,
                    EntityType.DATABASE_TIER, CloudService.AZURE_DATA_SERVICES,
                    EntityType.STORAGE_TIER, CloudService.AZURE_STORAGE
            );

    private static final Map<EntityType, EntityType> AZURE_PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE =
            ImmutableMap.of(
                    EntityType.VIRTUAL_MACHINE, EntityType.COMPUTE_TIER,
                    EntityType.DATABASE, EntityType.DATABASE_TIER
            );

    @Nonnull
    @Override
    public Map<EntityType, IEntityConverter> getEntityConverters() {
        return AZURE_ENTITY_CONVERTERS;
    }

    @Nonnull
    @Override
    public String getStorageTierId(@Nonnull String storageTierName) {
        return "azure::ST::" + storageTierName;
    }

    /**
     * Get region id based on AZ id. It get region name from AZ id and then combine
     * with prefix into the Region id. For example:
     *     Azure AZ id is:     azure::northcentralus::PM::northcentralus
     *     Azure Region id is: azure::northcentralus::DC::northcentralus
     *
     * @param azId id of the AZ
     * @return region id for the AZ id
     */
    @Nonnull
    @Override
    public String getRegionIdFromAzId(@Nonnull String azId) {
        String region = azId.split("::", 3)[1];
        return "azure::" + region + "::DC::" + region;
    }

    /**
     * Azure replaces "/" with "::" to construct volume id. For example:
     * "/subscriptions/758ad253-cbf5-4b18-8863-3eed0825bf07/resourceGroups/OLEGN_RG/
     * providers/Microsoft.Compute/disks/olga-stress_OsDisk_1_586cb62ed63c42ea84b7e3bc135c6473"
     *
     * "::subscriptions::758ad253-cbf5-4b18-8863-3eed0825bf07::resourcegroups::olegn_rg::
     * providers::microsoft.compute::disks::olga-stress_osdisk_1_586cb62ed63c42ea84b7e3bc135c6473"
     */
    @Nonnull
    @Override
    public Optional<String> getVolumeIdFromStorageFilePath(@Nullable String regionName,
                                                           @Nonnull String filePath) {
        return Optional.of(filePath.replace("/", "::").toLowerCase());
    }

    @Nonnull
    @Override
    public Optional<CloudService> getCloudServiceOwner(@Nonnull EntityType entityType) {
        return Optional.ofNullable(ENTITY_TYPE_OWNED_BY_CLOUD_SERVICE_MAP.get(entityType));
    }

    @Nonnull
    @Override
    public Set<CloudService> getCloudServicesToCreate() {
        return ConverterUtils.getCloudServicesByProbeType(SDKProbeType.AZURE);
    }

    @Override
    public Optional<EntityType> getCloudEntityTypeForProfileType(@Nonnull EntityType entityType) {
        return Optional.ofNullable(AZURE_PROFILE_TYPE_TO_CLOUD_ENTITY_TYPE.get(entityType));
    }
}
