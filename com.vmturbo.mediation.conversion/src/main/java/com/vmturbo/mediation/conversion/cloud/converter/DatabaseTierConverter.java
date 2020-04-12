package com.vmturbo.mediation.conversion.cloud.converter;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;

/**
 * The database tier converter. DatabaseTier is created based on db profile from the probe. It
 * creates sold commodities and connect DatabaseTier to Region based on license info in profile dto.
 *
 * For Azure, DatabaseTier is created from Database profile, which should sell:
 *     DBMem, Transaction, TransactionLog, Connection, DBCacheHitRate, ResponseTime
 */
public class DatabaseTierConverter implements IEntityConverter {

    /**
     * Mapping from sold commodities types (that need to be created for DatabaseTier) to commodity
     * types in the commodity profile where the capacity comes from.
     *
     * For example: Azure DatabaseTier should contain commodity DB_MEM which comes from the VMEM
     * CommodityProfile inside the EntityProfileDTO:
     *     commodityProfile {
     *         commodityType: VMEM
     *         capacity: 1.6384E8
     *     }
     *
     * More details: https://vmturbo.atlassian.net/wiki/spaces/Home/pages/735674634/Commodities+to+be+sold+by+the+market+tiers
     */
    private static final Map<CommodityType, CommodityType> SOLD_COMMODITY_TO_COMMODITY_PROFILE_MAPPING =
            ImmutableMap.of(
                    CommodityType.DB_MEM, CommodityType.VMEM,
                    CommodityType.TRANSACTION, CommodityType.VCPU,
                    CommodityType.TRANSACTION_LOG, CommodityType.IO_THROUGHPUT
            );

    /**
     * List of additional sold commodities types that need to be created for DatabaseTier for
     * different probe types. These commodities do not have info in EntityProfileDTO and should
     * be created separately.
     */
    private static final List<CommodityType> ADDITIONAL_SOLD_COMMODITIES = ImmutableList.of(
            CommodityType.CONNECTION,
            CommodityType.DB_CACHE_HIT_RATE,
            CommodityType.RESPONSE_TIME
    );

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        String databaseTierId = entity.getId();
        EntityProfileDTO profileDTO = converter.getProfileDTO(databaseTierId);

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

        // create additional sold commodities which are required by market but not in EntityProfileDTO
        ADDITIONAL_SOLD_COMMODITIES.forEach(commodityType ->
                entity.addCommoditiesSold(CommodityDTO.newBuilder()
                        .setCommodityType(commodityType)
                        .setCapacity(ConverterUtils.DEFAULT_ACCESS_COMMODITY_CAPACITY)
                        .build()));

        // owned by cloud service
        converter.ownedByCloudService(EntityType.DATABASE_TIER, databaseTierId);

        return true;
    }
}
