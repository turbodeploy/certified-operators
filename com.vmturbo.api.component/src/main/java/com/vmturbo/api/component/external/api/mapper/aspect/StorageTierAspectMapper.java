package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Mapper for getting storage tier aspect.
 */
public class StorageTierAspectMapper extends AbstractAspectMapper {

    @Override
    public EntityAspect mapEntityToAspect(@Nonnull TopologyEntityDTO entity) {
        // todo: map the common storage aspects
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        aspect.setDisplayName(entity.getDisplayName());
        aspect.setName(String.valueOf(entity.getOid()));
        return aspect;
    }

    @Override
    public @Nonnull AspectName getAspectName() {
        return AspectName.STORAGE_TIER;
    }
}
