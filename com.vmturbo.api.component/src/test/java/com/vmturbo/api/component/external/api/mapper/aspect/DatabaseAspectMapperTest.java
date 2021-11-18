package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.DBEntityAspectApiDTO;
import com.vmturbo.api.enums.ReplicationRole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

public class DatabaseAspectMapperTest extends BaseAspectMapperTest {

    private static final DatabaseEdition TEST_DATABASE_EDITION = DatabaseEdition.ENTERPRISE;
    private static final String RAW_DATABASE_EDITION = "Enterprise Edition";
    private static final DatabaseEngine TEST_DATABASE_ENGINE = DatabaseEngine.MARIADB;
    private static final DeploymentType TEST_DEPLOYMENT_TYPE = DeploymentType.MULTI_AZ;
    private static final LicenseModel TEST_LICENSE_MODEL = LicenseModel.BRING_YOUR_OWN_LICENSE;
    private static final String TEST_DATABASE_VERSION = "666";
    private static final Integer MAX_CONCURRENT_SESSION = 400;
    private static final Integer MAX_CONCURRENT_WORKER = 10;
    private static final String PRICING_MODEL = "DTU";
    private static final String STORAGE_AMOUNT = "2";
    private static final String DB_SERVER_NAME = "dbServer1";
    private static final ReplicationRole DB_REPLICATION_ROLE = ReplicationRole.Primary;

    private static final long TEST_OID = 123L;

    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.DATABASE,
            TypeSpecificInfo.newBuilder()
                .setDatabase(DatabaseInfo.newBuilder()
                    .setEdition(TEST_DATABASE_EDITION)
                    .setRawEdition(RAW_DATABASE_EDITION)
                    .setEngine(TEST_DATABASE_ENGINE)
                    .setVersion(TEST_DATABASE_VERSION)
                    .setDeploymentType(TEST_DEPLOYMENT_TYPE)
                    .setLicenseModel(TEST_LICENSE_MODEL))
                .build())
                .putEntityPropertyMap("max_concurrent_session", "400")
                .putEntityPropertyMap("max_concurrent_worker", "10")
                .putEntityPropertyMap("pricing_model", PRICING_MODEL)
                .putEntityPropertyMap("storage_amount", STORAGE_AMOUNT)
                .putEntityPropertyMap("replication_role", DB_REPLICATION_ROLE.name())
                .putEntityPropertyMap("DB_SERVER_NAME", DB_SERVER_NAME);

        final DatabaseAspectMapper mapper = new DatabaseAspectMapper();
        // act
        final DBEntityAspectApiDTO dbAspect = mapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertEquals(RAW_DATABASE_EDITION, dbAspect.getDbEdition());
        assertEquals(TEST_DATABASE_ENGINE.name(), dbAspect.getDbEngine());
        assertEquals(TEST_DATABASE_VERSION, dbAspect.getDbVersion());
        assertEquals(TEST_LICENSE_MODEL.name(), dbAspect.getLicenseModel());
        assertEquals(TEST_DEPLOYMENT_TYPE.name(), dbAspect.getDeploymentType());
        assertEquals(TEST_DEPLOYMENT_TYPE.name(), dbAspect.getDeploymentType());
        assertEquals(MAX_CONCURRENT_SESSION, dbAspect.getMaxConcurrentSessions());
        assertEquals(MAX_CONCURRENT_WORKER, dbAspect.getMaxConcurrentWorkers());
        assertEquals(PRICING_MODEL, dbAspect.getPricingModel());
        assertEquals(DB_SERVER_NAME, dbAspect.getDbServerName());
        assertEquals(DB_REPLICATION_ROLE, dbAspect.getReplicationRole());

    }
}
