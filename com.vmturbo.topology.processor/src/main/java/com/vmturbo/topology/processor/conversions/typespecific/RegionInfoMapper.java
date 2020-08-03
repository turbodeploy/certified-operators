package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.GeoDataInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.RegionInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.RegionData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.GeoData;

/**
 * Populate the {@link TypeSpecificInfo} unique to a RegionInfo - i.e. {@link RegionInfo}
 **/
public class RegionInfoMapper extends TypeSpecificInfoMapper {

    /**
     * Map a given entity dto to its type specific information.
     *
     * @param sdkEntity         the SDK {@link EntityDTO} for which we will build the {@link TypeSpecificInfo}
     * @param entityPropertyMap the mapping from property name to property value, which comes from
     *                          the {@link EntityDTO#entityProperties_}. For most cases, the type specific info is set in
     *                          {@link EntityDTO#entityData_}, but some are only set inside {@link EntityDTO#entityProperties_}
     * @return TypeSpecificInfo for the given sdk entity
     */
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                                           @Nonnull final Map<String, String> entityPropertyMap) {

        if (sdkEntity.hasRegionData()) {
            RegionData regionData = sdkEntity.getRegionData();
            if (regionData.hasGeoData()) {
                GeoData geoData = regionData.getGeoData();
                return TypeSpecificInfo.newBuilder()
                        .setRegion(
                                RegionInfo.newBuilder()
                                        .setGeoData(
                                                GeoDataInfo.newBuilder()
                                                        .setLatitude(geoData.getLatitude())
                                                        .setLongitude(geoData.getLongitude())
                                        ))
                        .build();
            }
        }

        return TypeSpecificInfo.newBuilder()
                .setRegion(RegionInfo.newBuilder())
                .build();
    }
}
