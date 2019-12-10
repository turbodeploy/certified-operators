package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.RegionAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.GeoDataInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.RegionInfo;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * RegionAspectMapper to map region to its aspects.
 * At present we only have GeoData attached into region aspects.
 */
public class RegionAspectMapper extends AbstractAspectMapper {

    /**
     * Map the topology entity dto to RegionAspectApiDTO.
     *
     * @param entity the entity to get aspect for
     * @return EntityAspect the aspect for the region entity
     */
    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
        if (typeSpecificInfo != null && typeSpecificInfo.hasRegion()) {
            final RegionInfo regionInfo = typeSpecificInfo.getRegion();
            if (regionInfo != null && regionInfo.hasGeoData()) {
                final GeoDataInfo geoDataInfo = regionInfo.getGeoData();
                final RegionAspectApiDTO regionAspectApiDTO = new RegionAspectApiDTO();
                regionAspectApiDTO.setLatitude(geoDataInfo.getLatitude());
                regionAspectApiDTO.setLongitude(geoDataInfo.getLongitude());
                return regionAspectApiDTO;
            }
        }
        return null;
    }

    /**
     * Get Aspect Name for this aspect.
     *
     * @return aspect name
     */
    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.REGION;
    }
}
