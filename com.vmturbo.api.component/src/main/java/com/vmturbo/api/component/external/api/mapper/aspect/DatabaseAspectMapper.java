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

    private static final String MAX_CONCURRENT_SESSION = "max_concurrent_session";
    private static final String MAX_CONCURRENT_WORKER = "max_concurrent_worker";
    private static final String PRICING_MODEL = "pricing_model";
    private static final String STORAGE_AMOUNT = "storage_amount";

    @Nullable
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final DBEntityAspectApiDTO aspect = new DBEntityAspectApiDTO();
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

        String storageAmount = entity.getEntityPropertyMapOrDefault(STORAGE_AMOUNT, null);
        if (storageAmount != null) {
            aspect.setStorageAmount(storageAmount);
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.DATABASE;
    }
}
