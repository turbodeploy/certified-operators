package com.vmturbo.mediation.cloud.converter;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;

/**
 * The database tier rewirer. DatabaseTier comes from profile:
 *     AWS:   db server profile
 *     Azure: db profile
 *
 * Connect DatabaseTier to Region based on license info in profile dto.
 *
 * CLOUD-TODO: figure out how to store info from EntityProfileDTO to EntityDTO
 * this is a general task for how to store entity specific information in XL
 */
public class DatabaseTierConverter implements IEntityConverter {

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        String databaseTierId = entity.getId();
        EntityProfileDTO profileDTO = converter.getProfileDTO(databaseTierId);

        // connect DT to Region, based on license
        profileDTO.getDbProfileDTO().getLicenseList().forEach(licenseMapEntry ->
                entity.addLayeredOver(licenseMapEntry.getRegion()));

        // create sold commodities, based on commodityProfile defined in entityProfile
        // todo: some commodityProfile only has consumed defined, but not capacity, discard for now?
        List<CommodityDTO> soldCommoditiesFromCommodityProfiles = profileDTO.getCommodityProfileList().stream()
                .filter(CommodityProfileDTO::hasCapacity)
                .map(commodityProfileDTO -> CommodityDTO.newBuilder()
                        .setCommodityType(commodityProfileDTO.getCommodityType())
                        .setCapacity(commodityProfileDTO.getCapacity())
                        .build())
                .collect(Collectors.toList());
        entity.addAllCommoditiesSold(soldCommoditiesFromCommodityProfiles);

        // owned by cloud service
        converter.ownedByCloudService(EntityType.DATABASE_TIER, databaseTierId);

        return true;
    }
}
