package com.vmturbo.mediation.cloud.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.ProfileDTO.CommodityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.DBProfileDTO;

/**
 * Helper class containing constants, util functions, etc, which are used by different converters.
 */
public class ConverterUtils {

    /**
     * Used to specify default access commodity capacity. As required by market, this needs to be
     * set to a very large number ("infinity"). This is used for setting the capacity of access
     * commodities (license, application, etc.) and some commodities (like: DBCacheHitRate) when
     * capacity is not available.
     */
    public static final float DEFAULT_ACCESS_COMMODITY_CAPACITY = 1.0E9f;

    /**
     * Create sold commodities based on CommodityProfile list in EntityProfileDTO.
     *
     * @param commodityProfiles list of {@link CommodityProfileDTO}s to create commodities based on
     * @param soldCommToCommProfileMapping the mapping from sold commodities types (that need to be
     * created) to commodity types in the commodity profile where the capacity comes from
     * @return list of sold commodities created from the commodityProfiles
     */
    public static List<CommodityDTO> createSoldCommoditiesFromCommodityProfiles(
            @Nonnull List<CommodityProfileDTO> commodityProfiles,
            @Nonnull Map<CommodityType, CommodityType> soldCommToCommProfileMapping) {
        final Map<CommodityType, Float> capacityMap = commodityProfiles.stream()
                .collect(Collectors.toMap(CommodityProfileDTO::getCommodityType,
                        CommodityProfileDTO::getCapacity));
        return soldCommToCommProfileMapping.entrySet().stream()
                .map(entry -> {
                    final CommodityType destCommType = entry.getKey();
                    final CommodityType srcCommType = entry.getValue();
                    CommodityDTO.Builder commBuilder = CommodityDTO.newBuilder()
                            .setCommodityType(destCommType);
                    // set capacity if there is CommodityProfile for this CommodityTypes
                    if (capacityMap.containsKey(srcCommType)) {
                        commBuilder.setCapacity(capacityMap.get(srcCommType));
                    }
                    return commBuilder.build();
                }).collect(Collectors.toList());
    }

    /**
     * Create sold commodities based on dbProfileDTO in EntityProfileDTO. Currently the following
     * commodities are created:
     *     APPLICATION, LICENSE_ACCESS
     *
     * @param dbProfileDTO the {@link DBProfileDTO} to create commodities based on
     * @return list of sold commodities created from the dbProfileDTO
     */
    public static List<CommodityDTO> createSoldCommoditiesFromDBProfileDTO(@Nonnull DBProfileDTO dbProfileDTO) {
        final List<CommodityDTO> soldCommodities = Lists.newArrayList();
        dbProfileDTO.getLicenseList().stream()
                .flatMap(licenseMapEntry -> {
                    final String regionId = licenseMapEntry.getRegion();
                        // create application commodity for each region
                    soldCommodities.add(CommodityDTO.newBuilder()
                                .setCommodityType(CommodityType.APPLICATION)
                                .setKey("Application::" + regionId)
                                .setCapacity(DEFAULT_ACCESS_COMMODITY_CAPACITY)
                                .build());
                    return licenseMapEntry.getLicenseNameList().stream();
                }).distinct()
                .forEach(license ->
                // create license access commodity for each distinct license
                soldCommodities.add(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.LICENSE_ACCESS)
                        .setKey(license)
                        .setCapacity(DEFAULT_ACCESS_COMMODITY_CAPACITY)
                        .build())
        );
        return soldCommodities;
    }
}
