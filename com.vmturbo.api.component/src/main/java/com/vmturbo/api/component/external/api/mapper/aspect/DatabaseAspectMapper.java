package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.api.dto.entityaspect.DBEntityAspectApiDTO;
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
    private static final String STORAGE_TIER = "storage_tier";
    private static final String DB_SERVER_NAME_PROPERTY = "DB_SERVER_NAME";
    private static final String DB_SERVICE_TIER = "server_service_tier";
    private static final String DB_COMPUTE_TIER = "server_compute_tier";
    private static final String DB_HW_GENERATION = "server_hardware_generation";

    private static final Map<String, BiConsumer<DBEntityAspectApiDTO, String>> SETTER_MAPPING =
            ImmutableMap.<String, BiConsumer<DBEntityAspectApiDTO, String>>builder()
                    .put(MAX_CONCURRENT_SESSION,
                            (aspect, v) -> aspect.setMaxConcurrentSessions(Integer.parseInt(v)))
                    .put(MAX_CONCURRENT_WORKER,
                            (aspect, v) -> aspect.setMaxConcurrentWorkers(Integer.parseInt(v)))
                    .put(PRICING_MODEL, DBEntityAspectApiDTO::setPricingModel)
                    .put(STORAGE_TIER, DBEntityAspectApiDTO::setStorageTier)
                    .put(DB_SERVER_NAME_PROPERTY, DBEntityAspectApiDTO::setDbServerName)
                    .put(DB_SERVICE_TIER, DBEntityAspectApiDTO::setServiceTier)
                    .put(DB_COMPUTE_TIER, DBEntityAspectApiDTO::setComputeTier)
                    .put(DB_HW_GENERATION, DBEntityAspectApiDTO::setHardwareGeneration)
                    .build();



    @Nullable
    @Override
    public DBEntityAspectApiDTO mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
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

        for (Entry<String, BiConsumer<DBEntityAspectApiDTO, String>> entry : SETTER_MAPPING.entrySet()) {
            final String propertyName = entry.getKey();
            final String value = entity.getEntityPropertyMapOrDefault(propertyName, null);
            if (StringUtils.isNotBlank(value)) {
                entry.getValue().accept(aspect, value);
            }
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.DATABASE;
    }
}
