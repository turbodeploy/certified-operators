package com.vmturbo.api.component.external.api.mapper.aspect;

import static com.vmturbo.common.protobuf.utils.StringConstants.DBS_STORAGE_TIER;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.entityaspect.DatabaseServerEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.ClusterRole;
import com.vmturbo.api.enums.DatabaseServerFeatures;
import com.vmturbo.api.enums.FeatureState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Topology Extension data related to Database type-specific data.
 **/
public class DatabaseServerAspectMapper extends AbstractAspectMapper {

    private static final Logger logger = LogManager.getLogger();

    private static final String MAX_CONCURRENT_SESSION = "max_concurrent_session";
    private static final String MAX_CONCURRENT_WORKER = "max_concurrent_worker";
    private static final String PRICING_MODEL = "pricing_model";

    @Nullable
    @Override
    public DatabaseServerEntityAspectApiDTO mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        final DatabaseServerEntityAspectApiDTO aspect = new DatabaseServerEntityAspectApiDTO();
        if (entity.getTypeSpecificInfo().hasDatabaseServer()) {
            final DatabaseServerInfo databaseServerInfo = entity.getTypeSpecificInfo().getDatabaseServer();
            if (databaseServerInfo.hasRawEdition()) {
                aspect.setDbEdition(databaseServerInfo.getRawEdition());
            }
            if (databaseServerInfo.hasEngine()) {
                aspect.setDbEngine(databaseServerInfo.getEngine().name());
            }
            if (databaseServerInfo.hasVersion()) {
                aspect.setDbVersion(databaseServerInfo.getVersion());
            }
            if (databaseServerInfo.hasLicenseModel()) {
                aspect.setLicenseModel(databaseServerInfo.getLicenseModel().name());
            }
            if (databaseServerInfo.hasDeploymentType()) {
                aspect.setDeploymentType(databaseServerInfo.getDeploymentType().name());
            }
            if (databaseServerInfo.hasHourlyBilledOps()) {
                aspect.setHourlyBilledOps(databaseServerInfo.getHourlyBilledOps());
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

        String storageTier = entity.getEntityPropertyMapOrDefault(DBS_STORAGE_TIER, null);
        if (storageTier != null) {
            aspect.setStorageTier(storageTier);
        }

        final String clusterRoleStr = entity.getEntityPropertyMapOrDefault(
                StringConstants.CLUSTER_ROLE, null);
        if (StringUtils.isNoneBlank(clusterRoleStr)) {
            try {
                final ClusterRole clusterRole = ClusterRole.valueOf(clusterRoleStr);
                aspect.setClusterRole(clusterRole);
            } catch (IllegalArgumentException ex) {
                logger.error("Cannot find ClusterRole for {}", clusterRoleStr);
            }
        }

        final String storageEncryption = entity.getEntityPropertyMapOrDefault(
                StringConstants.STORAGE_ENCRYPTION, null);
        final String storageAutoscaling = entity.getEntityPropertyMapOrDefault(
                StringConstants.STORAGE_AUTOSCALING, null);
        final String performanceInsights = entity.getEntityPropertyMapOrDefault(
                StringConstants.AWS_PERFORMANCE_INSIGHTS, null);
        getFeatureState(storageEncryption).ifPresent(v -> aspect.addFeature(DatabaseServerFeatures.StorageEncryption, v));
        getFeatureState(storageAutoscaling).ifPresent(v -> aspect.addFeature(DatabaseServerFeatures.StorageAutoscaling, v));
        getFeatureState(performanceInsights).ifPresent(v -> aspect.addFeature(DatabaseServerFeatures.PerformanceInsights, v));
        return aspect;
    }

    private Optional<FeatureState> getFeatureState(String value) {
        if (StringUtils.isNoneBlank(value)) {
            try {
                return Optional.of(FeatureState.valueOf(value));
            } catch (IllegalArgumentException ex) {
                logger.error("Cannot find FeatureState for {}", value);
            }
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.DATABASE_SERVER;
    }
}
