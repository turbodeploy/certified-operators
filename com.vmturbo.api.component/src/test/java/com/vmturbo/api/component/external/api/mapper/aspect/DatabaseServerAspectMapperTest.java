package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.DatabaseServerEntityAspectApiDTO;
import com.vmturbo.api.enums.ClusterRole;
import com.vmturbo.api.enums.DatabaseServerFeatures;
import com.vmturbo.api.enums.FeatureState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

/**
 * Tests to ensure {@link DatabaseServerAspectMapper} behaves as intended.
 */
public class DatabaseServerAspectMapperTest extends BaseAspectMapperTest {

    private static final DatabaseEdition TEST_DATABASE_EDITION = DatabaseEdition.ENTERPRISE;
    private static final String RAW_DATABASE_EDITION = "Enterprise Edition";
    private static final DatabaseEngine TEST_DATABASE_ENGINE = DatabaseEngine.MARIADB;
    private static final DeploymentType TEST_DEPLOYMENT_TYPE = DeploymentType.MULTI_AZ;
    private static final LicenseModel TEST_LICENSE_MODEL = LicenseModel.BRING_YOUR_OWN_LICENSE;
    private static final Double HOURLY_BILLED_OPS = 1234.0;
    private static final String TEST_DATABASE_VERSION = "666";
    private static final Integer MAX_CONCURRENT_SESSION = 400;
    private static final Integer MAX_CONCURRENT_WORKER = 10;
    private static final String PRICING_MODEL = "DTU";
    private static final String STORAGE_AMOUNT = "2";
    private static final String DB_SERVER_NAME = "dbServer1";
    private static final String CLUSTER_WRITER = "Writer";
    private static final String ENABLED = "Enabled";
    private static final String DISABLED = "Disabled";

    /**
     * Tests the mapping of a DatabaseServer entity to it's corresponding aspect.
     */
    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
                EntityType.DATABASE_SERVER,
                TypeSpecificInfo.newBuilder()
                        .setDatabaseServer(DatabaseServerInfo.newBuilder()
                                .setEdition(TEST_DATABASE_EDITION)
                                .setRawEdition(RAW_DATABASE_EDITION)
                                .setEngine(TEST_DATABASE_ENGINE)
                                .setVersion(TEST_DATABASE_VERSION)
                                .setDeploymentType(TEST_DEPLOYMENT_TYPE)
                                .setLicenseModel(TEST_LICENSE_MODEL)
                                .setHourlyBilledOps(HOURLY_BILLED_OPS))
                        .build())
                .putEntityPropertyMap("max_concurrent_session", "400")
                .putEntityPropertyMap("max_concurrent_worker", "10")
                .putEntityPropertyMap("pricing_model", PRICING_MODEL)
                .putEntityPropertyMap("storage_amount", STORAGE_AMOUNT)
                .putEntityPropertyMap("DB_SERVER_NAME", DB_SERVER_NAME)
                .putEntityPropertyMap(StringConstants.CLUSTER_ROLE, CLUSTER_WRITER)
                .putEntityPropertyMap(StringConstants.STORAGE_ENCRYPTION, ENABLED)
                .putEntityPropertyMap(StringConstants.STORAGE_AUTOSCALING, DISABLED)
                .putEntityPropertyMap(StringConstants.AWS_PERFORMANCE_INSIGHTS, "");

        final DatabaseServerAspectMapper mapper = new DatabaseServerAspectMapper();
        // act
        final DatabaseServerEntityAspectApiDTO databaseServerAspect = mapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertEquals(RAW_DATABASE_EDITION, databaseServerAspect.getDbEdition());
        assertEquals(TEST_DATABASE_ENGINE.name(), databaseServerAspect.getDbEngine());
        assertEquals(TEST_DATABASE_VERSION, databaseServerAspect.getDbVersion());
        assertEquals(TEST_LICENSE_MODEL.name(), databaseServerAspect.getLicenseModel());
        assertEquals(HOURLY_BILLED_OPS, databaseServerAspect.getHourlyBilledOps());
        assertEquals(TEST_DEPLOYMENT_TYPE.name(), databaseServerAspect.getDeploymentType());
        assertEquals(TEST_DEPLOYMENT_TYPE.name(), databaseServerAspect.getDeploymentType());
        assertEquals(MAX_CONCURRENT_SESSION, databaseServerAspect.getMaxConcurrentSessions());
        assertEquals(MAX_CONCURRENT_WORKER, databaseServerAspect.getMaxConcurrentWorkers());
        assertEquals(PRICING_MODEL, databaseServerAspect.getPricingModel());
        assertEquals(ClusterRole.Writer, databaseServerAspect.getClusterRole());
        assertEquals(FeatureState.Enabled, databaseServerAspect.getFeatureStateMap().get(
                DatabaseServerFeatures.StorageEncryption));
        assertEquals(FeatureState.Disabled, databaseServerAspect.getFeatureStateMap().get(
                DatabaseServerFeatures.StorageAutoscaling));
        assertEquals(null, databaseServerAspect.getFeatureStateMap().get(
                DatabaseServerFeatures.PerformanceInsights));
    }
}