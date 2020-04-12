package com.vmturbo.mediation.conversion.cloud.converter;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;

/**
 * The database server tier converter. DatabaseServerTier is created based on db server profile
 * from the probe. It creates sold commodities and connect DatabaseServerTier to Region based on
 * license info in profile dto.
 *
 * For AWS, DatabaseServerTier is created from DatabaseServer profile, which should sell:
 *     vMem, vCPU, vStorage, ioThroughput
 */
public class DatabaseServerTierConverter implements IEntityConverter {

    /**
     * Mapping from sold commodities types (that need to be created for DatabaseServerTier) to
     * commodity types in the commodity profile where the capacity comes from.
     *
     * For example: AWS DatabaseServerTier should sell commodity IO_THROUGHPUT which comes from the
     * IO_THROUGHPUT CommodityProfile inside the EntityProfileDTO:
     *     commodityProfile {
     *         commodityType: IO_THROUGHPUT
     *         capacity: 2000000.0
     *     }
     *
     * More details: https://vmturbo.atlassian.net/wiki/spaces/Home/pages/735674634/Commodities+to+be+sold+by+the+market+tiers
     */
    private static final Map<CommodityType, CommodityType> SOLD_COMMODITY_TO_COMMODITY_PROFILE_MAPPING =
            ImmutableMap.of(
                    CommodityType.VMEM, CommodityType.VMEM,
                    CommodityType.VCPU, CommodityType.VCPU,
                    CommodityType.VSTORAGE, CommodityType.VSTORAGE,
                    CommodityType.IO_THROUGHPUT, CommodityType.IO_THROUGHPUT
            );

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        String databaseServerTierId = entity.getId();
        EntityProfileDTO profileDTO = converter.getProfileDTO(databaseServerTierId);

        // connect DT to Region, based on license
        profileDTO.getDbProfileDTO().getLicenseList().forEach(licenseMapEntry ->
                entity.addLayeredOver(licenseMapEntry.getRegion()));

        // create sold commodities based on CommodityProfile in EntityProfileDTO
        entity.addAllCommoditiesSold(ConverterUtils.createSoldCommoditiesFromCommodityProfiles(
                profileDTO.getCommodityProfileList(),
                SOLD_COMMODITY_TO_COMMODITY_PROFILE_MAPPING));

        // create sold commodities based on dbProfileDTO in EntityProfileDTO
        entity.addAllCommoditiesSold(ConverterUtils.createSoldCommoditiesFromDBProfileDTO(
                profileDTO.getDbProfileDTO()));

        // owned by cloud service
        converter.ownedByCloudService(EntityType.DATABASE_SERVER_TIER, databaseServerTierId);

        return true;
    }
}
