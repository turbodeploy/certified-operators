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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

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
     * Get the region id based on the zone id. This should be implemented by different probes.
     *
     * @param azId id of the availability zone
     * @return id of the related region
     */
    @Nonnull
    String getRegionIdFromAzId(@Nonnull String azId);

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
}
