package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityCapacityLimit;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseServerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;

/**
 * Populate the {@link TypeSpecificInfo} unique to an DatabaseServer - i.e. {@link DatabaseServerInfo}.
 **/
public class DatabaseServerInfoMapperTest {

    private static final IpAddress TEST_IP_ADDRESS = IpAddress.newBuilder()
        .setIpAddress("1.2.3.4")
        .build();
    private static final DatabaseEngine DATABASE_ENGINE = DatabaseEngine.MARIADB;
    private static final DatabaseEdition DATABASE_EDITION = DatabaseEdition.ENTERPRISE;
    private static final String DATABASE_VERSION = "1.0";
    private static final String LICENSE_MODEL = "NoLicenseRequired";
    private static final String DEPLOYMENT_TYPE = "MultiAz";
    private static final double lowerBoundForResizeUp = 110f;
    private static final CommodityCapacityLimit COMMODITY_CAPACITY_LIMIT = CommodityCapacityLimit.newBuilder()
        .setCapacity((float)lowerBoundForResizeUp)
        .setCommodityType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();
    private static final float HOURLY_BILLED_OPS = 500f;




    /**
     * If the DB Server Data is set, then this is a DatabaseServer, and DatabaseServerInfoMapper is
     * the preferred way to set to extract DatabaseServerInfo.
     */
    @Test
    public void testExtractDbTypeSpecificInfoWithDBServerProperties() {
        // arrange
        final EntityDTOOrBuilder databaseServerEntityDTO = EntityDTO.newBuilder()
            .setApplicationData(ApplicationData.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS.getIpAddress())
                .setDbServerData(DatabaseServerData.newBuilder()
                    .setEngine(DATABASE_ENGINE.name())
                    .setEdition(DATABASE_EDITION.name())
                    .setVersion(DATABASE_VERSION)
                    .setLicenseModel(LICENSE_MODEL)
                    .setDeploymentType(DEPLOYMENT_TYPE)
                    .addLowerBoundScaleUp(COMMODITY_CAPACITY_LIMIT)
                    .setHourlyBilledOps(HOURLY_BILLED_OPS))
                .build());

        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setApplication(ApplicationInfo.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS)
                .build())
            .setDatabaseServer(DatabaseServerInfo.newBuilder()
                .setEngine(DATABASE_ENGINE)
                .setEdition(DATABASE_EDITION)
                .setRawEdition(DATABASE_EDITION.name())
                .setVersion(DATABASE_VERSION)
                .setLicenseModel(CloudCostDTO.LicenseModel.NO_LICENSE_REQUIRED)
                .setDeploymentType(CloudCostDTO.DeploymentType.MULTI_AZ)
                .addLowerBoundScaleUp(COMMODITY_CAPACITY_LIMIT)
                .setHourlyBilledOps(HOURLY_BILLED_OPS)
            ).build();

        final DatabaseServerInfoMapper testBuilder = new DatabaseServerInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(databaseServerEntityDTO,
            Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}


