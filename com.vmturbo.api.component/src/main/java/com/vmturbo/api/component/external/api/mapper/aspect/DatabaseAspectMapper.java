package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.DBEntityAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;

/**
 * Topology Extension data related to Database type-specific data.
 **/
public class DatabaseAspectMapper implements IAspectMapper {
    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final DBEntityAspectApiDTO aspect = new DBEntityAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasDatabase()) {
            final DatabaseInfo databaseInfo = entity.getTypeSpecificInfo().getDatabase();
            if (databaseInfo.hasEdition()) {
                aspect.setDbEdition(databaseInfo.getEdition().name());
            }
            if (databaseInfo.hasEngine()) {
                aspect.setDbEngine(databaseInfo.getEngine().name());
            }
            // todo: fill in the other fields
        }
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "dbAspect";
    }
}
