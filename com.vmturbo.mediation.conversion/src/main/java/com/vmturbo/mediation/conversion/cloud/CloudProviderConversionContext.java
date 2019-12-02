package com.vmturbo.mediation.conversion.cloud;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.conversion.util.CloudService;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 *
 */
public interface CloudProviderConversionContext {

    /**
     * Get the id for the storage tier. This should be implemented by different probes.
     *
     * @param storageTierName name of the storage tier
     * @return id of the storage tier
     */
    @Nonnull
    String getStorageTierId(@Nonnull String storageTierName);

    /**
     * Get the storage tier from the EntityDTO of a storage.
     *
     * @param storageDTO {@link EntityDTO} of a storage.
     * @return StorageTier corresponding to the storage
     */
    @Nonnull
    default String getStorageTier(@Nonnull EntityDTO.Builder storageDTO) {
        return storageDTO.getStorageData().getStorageTier();
    }

    /**
     * Get the region id based on the zone id. This should be implemented by different probes.
     *
     * @param azId id of the availability zone
     * @return id of the related region
     */
    @Nonnull
    String getRegionIdFromAzId(@Nonnull String azId);

    /**
     * Get the zone id based on the region id. This should be implemented by different probes.
     *
     * @param regionId id of the related region
     * @return id of the availability zone
     */
    @Nonnull
    String getAzIdFromRegionId(@Nonnull String regionId);

    /**
     * Get the optional volume id used by the probe based on the region name and the file path.
     *
     * @param regionName name of region
     * @param filePath path of storage file for this volume
     * @return optional id of the volume
     */
    @Nonnull
    Optional<String> getVolumeIdFromStorageFilePath(@Nonnull String regionName, @Nonnull String filePath);

    /**
     * Get all the converters for each entity type. This should be implemented by different
     * probes to return different converters based on the entity they discover.
     *
     * @return map of entity specific converter
     */
    @Nonnull
    Map<EntityType, IEntityConverter> getEntityConverters();

    /**
     * Get all the cloud services that need to be created for the probe.
     *
     * @return set of cloud services to be created
     */
    @Nonnull
    Set<CloudService> getCloudServicesToCreate();

    /**
     * Get the owner (cloud service) of the given entity type. This should be implemented by
     * different probes.
     *
     * @param entityType entity type to get owner for
     * @return optional cloud service which should own this entity type
     */
    Optional<CloudService> getCloudServiceOwner(@Nonnull EntityType entityType);

    /**
     * Get the new cloud entity type for the profile. This should be implemented by different probes.
     *
     * @param entityType type of the profile
     * @return new optional entity type to create EntityDTO
     */
    Optional<EntityType> getCloudEntityTypeForProfileType(@Nonnull EntityType entityType);

    /**
     * Get AvailabilityZone string from storage entity DTO.
     *
     * @param entity The Builder for the EntityDTO of a storage.
     * @return String giving the availability zone or an analagous String for cloud providers that
     * don't use availability zones.
     */
    default Optional<String> getAvailabilityZone(Builder entity) {
        return entity.getCommoditiesSoldList().stream()
            .filter(commodity -> commodity.getCommodityType() == CommodityType.DSPM_ACCESS)
            .map(commodityDTO -> CloudDiscoveryConverter.keyToUuid(commodityDTO.getKey()))
            .findAny();
    }
}
