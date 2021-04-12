package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.entityaspect.DatabaseServerEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;

/**
 * Topology Extension data related to Database type-specific data.
 **/
public class DatabaseServerAspectMapper extends AbstractAspectMapper {

    private static final String MAX_CONCURRENT_SESSION = "max_concurrent_session";
    private static final String MAX_CONCURRENT_WORKER = "max_concurrent_worker";
    private static final String PRICING_MODEL = "pricing_model";
    private static final String STORAGE_TIER = "storage_tier";


    @Nullable
    @Override
    public DatabaseServerEntityAspectApiDTO mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final DatabaseServerEntityAspectApiDTO aspect = new DatabaseServerEntityAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasDatabase()) {
            final DatabaseInfo databaseInfo = entity.getTypeSpecificInfo().getDatabase();
            if (databaseInfo.hasRawEdition()) {
                aspect.setDbEdition(databaseInfo.getRawEdition());
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

        String concurrentSessions = entity.getEntityPropertyMapOrDefault(MAX_CONCURRENT_SESSION, null);
        if (concurrentSessions != null) {
            aspect.setMaxConcurrentSessions(Integer.parseInt(concurrentSessions));
        }

        String concurrentWorkers = entity.getEntityPropertyMapOrDefault(MAX_CONCURRENT_WORKER, null);
        if (concurrentWorkers != null) {
            aspect.setMaxConcurrentWorkers(Integer.parseInt(concurrentWorkers));
        }

        String pricingModel = entity.getEntityPropertyMapOrDefault(PRICING_MODEL, null);
        if (pricingModel != null) {
            aspect.setPricingModel(pricingModel);
        }

        String storageTier = entity.getEntityPropertyMapOrDefault(STORAGE_TIER, null);
        if (storageTier != null) {
            aspect.setStorageTier(storageTier);
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.DATABASE_SERVER;
    }
}
