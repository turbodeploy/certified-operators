package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.DatabaseServerTierAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Topology Extension data related to Database server tier type-specific data.
 */
public class DatabaseServerTierAspectMapper extends AbstractAspectMapper {
    /**
     * Map entity to database server tier aspect.
     *
     * @param entity the {@link TopologyDTO.TopologyEntityDTO} to get aspect for
     * @return DatabaseServerTierAspectApiDTO
     */
    @Override
    @Nullable
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyDTO.TopologyEntityDTO entity) {
        if (entity.getEntityType() != CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_TIER_VALUE) {
            return null;
        }
        DatabaseServerTierAspectApiDTO databaseServerTierAspectApiDTO = new DatabaseServerTierAspectApiDTO();
        TopologyDTO.TypeSpecificInfo.DatabaseServerTierInfo databaseServerTierInfo = entity.getTypeSpecificInfo()
                .getDatabaseServerTier();
        databaseServerTierAspectApiDTO.setTierFamily(databaseServerTierInfo.getFamily());
        return databaseServerTierAspectApiDTO;
    }

    @Override
    @Nonnull
    public AspectName getAspectName() {
        return AspectName.DATABASE_SERVER_TIER;
    }
}
