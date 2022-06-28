package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData.Platform;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData.Tier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;

public class ApplicationInfoMapperTest {

    @Rule
    public TestName testName = new TestName();
    private static final Logger logger = LogManager.getLogger();

    private static final IpAddress TEST_IP_ADDRESS = IpAddress.newBuilder()
        .setIpAddress("1.2.3.4")
        .build();
    private static final DatabaseEngine DATABASE_ENGINE = DatabaseEngine.MARIADB;
    private static final DatabaseEdition DATABASE_EDITION = DatabaseEdition.ENTERPRISE;
    private static final String DATABASE_VERSION = "1.0";
    private static final String LICENSE_MODEL = "NoLicenseRequired";
    private static final String DEPLOYMENT_TYPE = "MultiAz";
    private static final String PLATFORM = "linux";
    private static final String TIER = "Standard";
    private static final int APP_COUNT = 4;
    private static final int INSTANCE_COUNT = 21;
    private static final int MAX_INSTANCE_COUNT = 30;
    private static final boolean ZONE_REDUNDANT = true;

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
        assertExtractedTypeSpecificInfo(applicationEntityDTO, expected);
    }

    @Test
    public void testExtractEmptyTypeSpecificInfo() {
        // Tests that there is no issue mapping an empty DTO object
        // arrange
        final EntityDTOOrBuilder applicationEntityDTO = EntityDTO.newBuilder()
                .setApplicationData(ApplicationData.newBuilder()
                        .setDbData(DatabaseData.newBuilder())
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setApplication(ApplicationInfo.newBuilder().build())
                .setDatabase(DatabaseInfo.newBuilder().build())
                .build();
        assertExtractedTypeSpecificInfo(applicationEntityDTO, expected);
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
                .setRawEdition(DATABASE_EDITION.name())
                .setVersion(DATABASE_VERSION)
                .build())
            .build();
        assertExtractedTypeSpecificInfo(applicationEntityDTO, expected);
    }

    /**
     * This test checks if license model and deployment type is converted correctly.
     */
    @Test
    public void testExtractDbTypeSpecificInfoWithLicenseModelAnDeploymentType() {
        // arrange
        final EntityDTOOrBuilder applicationEntityDTO = EntityDTO.newBuilder()
            .setApplicationData(ApplicationData.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS.getIpAddress())
                .setDbData(DatabaseData.newBuilder()
                    .setEngine(DATABASE_ENGINE.name())
                    .setEdition(DATABASE_EDITION.name())
                    .setVersion(DATABASE_VERSION)
                    .setLicenseModel(LICENSE_MODEL)
                    .setDeploymentType(DEPLOYMENT_TYPE)
                ));
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setApplication(ApplicationInfo.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS)
                .build())
            .setDatabase(DatabaseInfo.newBuilder()
                .setEngine(DATABASE_ENGINE)
                .setEdition(DATABASE_EDITION)
                .setRawEdition(DATABASE_EDITION.name())
                .setVersion(DATABASE_VERSION)
                .setLicenseModel(CloudCostDTO.LicenseModel.NO_LICENSE_REQUIRED)
                .setDeploymentType(CloudCostDTO.DeploymentType.MULTI_AZ))
            .build();
        assertExtractedTypeSpecificInfo(applicationEntityDTO, expected);
    }

    /**
     * This test runs the DB conversion with invalid license model and deployment profile.
     */
    @Test
    public void testExtractDbTypeSpecificInfoWithUnsupportedLicenseModelAnDeploymentType() {
        // arrange
        final EntityDTOOrBuilder applicationEntityDTO = EntityDTO.newBuilder()
            .setApplicationData(ApplicationData.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS.getIpAddress())
                .setDbData(DatabaseData.newBuilder()
                    .setEngine(DATABASE_ENGINE.name())
                    .setEdition(DATABASE_EDITION.name())
                    .setVersion(DATABASE_VERSION)
                    .setLicenseModel("Test")
                    .setDeploymentType("Test")
                ));
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setApplication(ApplicationInfo.newBuilder()
                .setIpAddress(TEST_IP_ADDRESS)
                .build())
            .setDatabase(DatabaseInfo.newBuilder()
                .setEngine(DATABASE_ENGINE)
                .setEdition(DATABASE_EDITION)
                .setRawEdition(DATABASE_EDITION.name())
                .setVersion(DATABASE_VERSION))
            .build();
        assertExtractedTypeSpecificInfo(applicationEntityDTO, expected);
    }

    @Test
    // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
    //  Remove this test when legacy support removed
    public void testExtractAppServiceInfo() {
        final EntityDTOOrBuilder appDto = EntityDTO.newBuilder().
                setApplicationServiceData(ApplicationServiceData.newBuilder()
                                .setAppCount(APP_COUNT)
                                .setCurrentInstanceCount(INSTANCE_COUNT)
                                .setPlatform(Platform.valueOf(PLATFORM.toUpperCase()))
                                .setTier(Tier.valueOf(TIER.toUpperCase()))
                                .setMaxInstanceCount(MAX_INSTANCE_COUNT)
                                .setZoneRedundant(ZONE_REDUNDANT)
                                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setApplicationService(ApplicationServiceInfo.newBuilder()
                        .setAppCount(APP_COUNT)
                        .setCurrentInstanceCount(INSTANCE_COUNT)
                        .setPlatform(TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo.Platform.valueOf(PLATFORM.toUpperCase()))
                        .setTier(TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo.Tier.valueOf(TIER.toUpperCase()))
                        .setMaxInstanceCount(MAX_INSTANCE_COUNT)
                        .setZoneRedundant(ZONE_REDUNDANT)
                        .build())
                .build();
        assertExtractedTypeSpecificInfo(appDto, expected);
    }

    @Test
    // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
    //  Remove this test when legacy support removed
    public void testExtractAppServiceDataMissingFields() {
        final EntityDTOOrBuilder appDto = EntityDTO.newBuilder()
                .setApplicationServiceData(ApplicationServiceData.newBuilder()
                        .setAppCount(APP_COUNT)
                        .setCurrentInstanceCount(INSTANCE_COUNT)
                        .setZoneRedundant(ZONE_REDUNDANT)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setApplicationService(ApplicationServiceInfo.newBuilder()
                        .setAppCount(APP_COUNT)
                        .setCurrentInstanceCount(INSTANCE_COUNT)
                        .setZoneRedundant(ZONE_REDUNDANT)
                        .build())
                .build();
        assertExtractedTypeSpecificInfo(appDto, expected);
    }

    private void assertExtractedTypeSpecificInfo(EntityDTOOrBuilder appDto, TypeSpecificInfo expected) {
        // act
        final ApplicationInfoMapper mapper = new ApplicationInfoMapper();
        TypeSpecificInfo actual = mapper.mapEntityDtoToTypeSpecificInfo(appDto, Collections.emptyMap());
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Test: %s\n", testName.getMethodName()));
        sb.append(String.format("Entity DTO: \n%s\n", appDto));
        sb.append(String.format("Expected topology DTO: \n%s\n", expected));
        sb.append(String.format("Actual topology DTO: \n%s", actual));
        logger.info(sb.toString());
        // assert
        assertThat("Expected type specific info does not match actual type specific info",
                actual, equalTo(expected));
    }
}
