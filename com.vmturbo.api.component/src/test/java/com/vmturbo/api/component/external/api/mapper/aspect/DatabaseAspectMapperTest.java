package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.*;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.DBEntityAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;

public class DatabaseAspectMapperTest extends BaseAspectMapperTest {

    public static final DatabaseEdition TEST_DATABASE_EDITION = DatabaseEdition.ORACLE_ENTERPRISE;
    public static final DatabaseEngine TEST_DATABASE_ENGINE = DatabaseEngine.MARIADB;
    public static final DeploymentType TEST_DEPLOYMENT_TYPE = DeploymentType.MULTI_AZ;
    public static final LicenseModel TEST_LICENSE_MODEL = LicenseModel.BRING_YOUR_OWN_LICENSE;
    private static final long TEST_OID = 123L;

    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyEntityDTO.Builder topologyEntityDTO = topologyEntityDTOBuilder(
            EntityType.DATABASE,
            TypeSpecificInfo.newBuilder()
                .setDatabase(DatabaseInfo.newBuilder()
                    .setEdition(TEST_DATABASE_EDITION)
                    .setEngine(TEST_DATABASE_ENGINE)
                    .setDeploymentType(TEST_DEPLOYMENT_TYPE)
                    .setLicenseModel(TEST_LICENSE_MODEL))
                .build());
        final DatabaseAspectMapper mapper = new DatabaseAspectMapper();
        // act
        EntityAspect result = mapper.mapEntityToAspect(topologyEntityDTO.build());
        // assert
        assertTrue(result instanceof DBEntityAspectApiDTO);
        DBEntityAspectApiDTO dbAspect = (DBEntityAspectApiDTO) result;
        assertEquals(TEST_DATABASE_EDITION.name(), dbAspect.getDbEdition());
        assertEquals(TEST_DATABASE_ENGINE.name(), dbAspect.getDbEngine());
        // note that the other database aspect field (dbVersion) is not set yet in the mapper
    }
}