package com.vmturbo.mediation.azure.volumes;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.azure.AzureConversionContext;
import com.vmturbo.mediation.azure.AzureStorageConverter;
import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * A subclass of AzureConversionContext that knows how to extract information from Azure Volume
 * discovered entities.
 */
public class AzureVolumesConversionContext extends AzureConversionContext {

    // for Azure Volumes we only care about converting files inside storage_data to volumes
    private static final Map<EntityType, IEntityConverter> AZURE_STORAGE_BROWSING_ENTITY_CONVERTERS =
        ImmutableMap.of(EntityType.STORAGE,
            new AzureStorageConverter(SDKProbeType.AZURE_STORAGE_BROWSE));

    @Nonnull
    @Override
    public Map<EntityType, IEntityConverter> getEntityConverters() {
        return AZURE_STORAGE_BROWSING_ENTITY_CONVERTERS;
    }

    @Nonnull
    @Override
    public Set<CloudService> getCloudServicesToCreate() {
        return Collections.emptySet();
    }

    /**
     * While Azure doesn't have the concept of AvailabilityZone we use the string ST::region in
     * its place.  Since the azure volumes probe will provide a property called lunuuid with a value
     * such as azure::ST::westus-unmanaged_standard, we parse out the section ST::westus, for
     * example, and return it here.
     *
     * @param entity The Builder for the EntityDTO of a storage.
     * @return
     */
    @Override
    public Optional<String> getAvailabilityZone(final Builder entity) {
        return entity.getEntityPropertiesList().stream()
            .filter(entityProp -> entityProp.getName().equals("lunuuid"))
            .map(EntityProperty::getValue)
            .map(lunuuid -> lunuuid.split("-")[0])
            .map(CloudDiscoveryConverter::keyToUuid)
            .findFirst();
    }

    /**
     * Azure volumes probe returns the storage tier as a property called storageType in the DTO.
     *
     * @param storageDTO {@link EntityDTOOrBuilder} of a storage.
     * @return Storage tier name.
     */
    @Nonnull
    @Override
    public String getStorageTier(@Nonnull final EntityDTOOrBuilder storageDTO) {
        return storageDTO.getEntityPropertiesList().stream()
            .filter(entityProp -> entityProp.getName().equals("storageType"))
            .map(EntityProperty::getValue)
            .findFirst()
            .get();
    }

    @Override
    public Optional<EntityType> getCloudEntityTypeForProfileType(@Nonnull final EntityType entityType) {
        return Optional.empty();
    }

    /**
     * Extract the region id which will be of the form azure::region::DC::region from the
     * availability zone id which will be of the form ST::region.
     *
     * @param azId id of the AZ
     * @return
     */
    @Nonnull
    @Override
    public String getRegionIdFromAzId(@Nonnull final String azId) {
        // for Azure Storage Browsing need to parse region out of string of the form
        // azure::ST::<region>-<storage tier>
        String region = azId.split("::")[1];
        return "azure::" + region + "::DC::" + region;
    }

    @Nonnull
    @Override
    public Optional<CloudService> getCloudServiceOwner(@Nonnull final EntityType entityType) {
        return Optional.empty();
    }
}
