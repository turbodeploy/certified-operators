package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import com.vmturbo.api.dto.entityaspect.DBEntityAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;

/**
 * Topology Extension data related to Database type-specific data.
 **/
public class DatabaseAspectMapper extends AbstractAspectMapper {
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
            if (databaseInfo.hasVersion()) {
                aspect.setDbVersion(databaseInfo.getVersion());
            }
            if (databaseInfo.hasLicenseModel()) {
                aspect.setLicenseModel(databaseInfo.getLicenseModel().name());
            }
            if (databaseInfo.hasDeploymentType()) {
                aspect.setDeploymentType(databaseInfo.getDeploymentType().name());
            }
        }

        String concurrentSession = entity.getEntityPropertyMapOrDefault("max_concurrent_session", null);
        if (concurrentSession != null) {
            aspect.setMaxConcurrentSession(Integer.parseInt(concurrentSession));
        }

        String concurrentWorker = entity.getEntityPropertyMapOrDefault("max_concurrent_worker", null);
        if (concurrentWorker != null) {
            aspect.setMaxConcurrentWorker(Integer.parseInt(concurrentWorker));
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.DATABASE;
    }
}
