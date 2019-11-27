package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;

public class ApplicationInfoMapperTest {


    private static final IpAddress TEST_IP_ADDRESS = IpAddress.newBuilder()
        .setIpAddress("1.2.3.4")
        .build();
    private static final DatabaseEngine DATABASE_ENGINE = DatabaseEngine.MARIADB;
    private static final DatabaseEdition DATABASE_EDITION = DatabaseEdition.ENTERPRISE;
    private static final String DATABASE_VERSION = "1.0";

    @Test
    public void testExtractTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder applicationEntityDTO = EntityDTO.newBuilder()
            .setApplicationData(ApplicationData.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS.getIpAddress())
                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setApplication(ApplicationInfo.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS)
                .build())
            .build();
        final ApplicationInfoMapper testBuilder = new ApplicationInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(applicationEntityDTO,
            Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }

    /**
     * If the DB Data is set, then this is a Database Application. The ApplicationInfo will
     * be set as well as the DatabaseInfo.
     */
    @Test
    public void testExtractDbTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder applicationEntityDTO = EntityDTO.newBuilder()
            .setApplicationData(ApplicationData.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS.getIpAddress())
                .setDbData(DatabaseData.newBuilder()
                    .setEngine(DATABASE_ENGINE.name())
                    .setEdition(DATABASE_EDITION.name())
                    .setVersion(DATABASE_VERSION))
                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setApplication(ApplicationInfo.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS)
                .build())
            .setDatabase(DatabaseInfo.newBuilder()
                .setEngine(DATABASE_ENGINE)
                .setEdition(DATABASE_EDITION)
                .setVersion(DATABASE_VERSION)
                .build())
            .build();
        final ApplicationInfoMapper testBuilder = new ApplicationInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(applicationEntityDTO,
            Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}