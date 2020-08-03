package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.DatabaseTierAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Topology Extension data related to Database tier type-specific data.
 */
public class DatabaseTierAspectMapper extends AbstractAspectMapper {
    /**
     * Map entity to database tier aspect.
     *
     * @param entity the {@link TopologyDTO.TopologyEntityDTO} to get aspect for
     * @return DatabaseTierAspectApiDTO
     */
    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyDTO.TopologyEntityDTO entity) {
        if (entity.getEntityType() != CommonDTO.EntityDTO.EntityType.DATABASE_TIER_VALUE) {
            return null;
        }
        DatabaseTierAspectApiDTO databaseTierAspectApiDTO = new DatabaseTierAspectApiDTO();
        TopologyDTO.TypeSpecificInfo.DatabaseTierInfo databaseTierInfo = entity.getTypeSpecificInfo().getDatabaseTier();
        databaseTierAspectApiDTO.setTierFamily(databaseTierInfo.getFamily());
        return databaseTierAspectApiDTO;
    }

    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.DATABASE_TIER;
    }
}
