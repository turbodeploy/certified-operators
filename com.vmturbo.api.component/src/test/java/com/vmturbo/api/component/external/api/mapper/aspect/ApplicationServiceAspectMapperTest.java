package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.vmturbo.api.dto.entityaspect.ApplicationServiceAspectApiDTO;
import com.vmturbo.api.enums.ApplicationServiceTier;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test the app component aspect mapping.
 */
public class ApplicationServiceAspectMapperTest extends BaseAspectMapperTest {

    /**
     * Junit TestName to allow the test names to be used in tests.
     */
    @Rule
    public TestName testName = new TestName();

    private static final Logger logger = LogManager.getLogger();

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final Integer MAX_INSTANCE_COUNT = 30;
    private static final Integer INSTANCE_COUNT = 10;
    private static final String TIER = "PREMIUMV3";
    private static final String PLATFORM = "LINUX";
    private static final boolean ZONE_REDUNDANT = true;
    private static final int APP_COUNT = 5;
    private static final int DAYS_EMPTY = 22;

    /**
     * Test standard usage of mapping an entity to app component aspect.
     */
    // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
    //  can remove APPLICATION_COMPONENT_VALUE when legacy support not needed
    @Test
    public void testMapEntityToAspectLegacyModel() {
        testMapEntityToAspect(EntityType.APPLICATION_COMPONENT);
    }

    /**
     * Test standard usage of mapping an entity to app component aspect.
     */
    @Test
    public void testMapEntityToAspect() {
        testMapEntityToAspect(EntityType.VIRTUAL_MACHINE_SPEC);
    }

    private void testMapEntityToAspect(EntityType entityType) {
        // arrange
        final TopologyEntityDTO.Builder tpEntityDtoBuilder = topologyEntityDTOBuilder(
                entityType,
                TypeSpecificInfo.newBuilder()
                        .setApplicationService(ApplicationServiceInfo.newBuilder()
                                .setPlatform(ApplicationServiceInfo.Platform.valueOf(PLATFORM))
                                .setTier(ApplicationServiceInfo.Tier.valueOf(TIER))
                                .setMaxInstanceCount(MAX_INSTANCE_COUNT)
                                .setCurrentInstanceCount(INSTANCE_COUNT)
                                .setAppCount(APP_COUNT)
                                .setZoneRedundant(ZONE_REDUNDANT)
                                .setDaysEmpty(DAYS_EMPTY)
                                .build())
                        .build());
        final ApplicationServiceAspectApiDTO expected = new ApplicationServiceAspectApiDTO();
        expected.setPlatform(Platform.valueOf(PLATFORM));
        expected.setTier(ApplicationServiceTier.valueOf(TIER));
        expected.setMaxInstanceCount(MAX_INSTANCE_COUNT);
        expected.setInstanceCount(INSTANCE_COUNT);
        expected.setAppCount(APP_COUNT);
        expected.setZoneRedundant(ZONE_REDUNDANT);
        expected.setDaysEmpty(DAYS_EMPTY);
        assertApplicationServiceAspect(tpEntityDtoBuilder.build(), expected);
    }

    /**
     * Verify no problems when fields are missing from the application info.
     */
    @Test
    public void testMapEntityToAspectMissingFieldsLegacyModel() {
        testMapEntityToAspectMissingFields(EntityType.APPLICATION_COMPONENT);
    }

    /**
     * Verify no problems when fields are missing from the application info.
     */
    @Test
    public void testMapEntityToAspectMissingFields() {
        testMapEntityToAspectMissingFields(EntityType.VIRTUAL_MACHINE_SPEC);
    }

    private void testMapEntityToAspectMissingFields(EntityType entityType) {
        // arrange
        final TopologyEntityDTO.Builder tpEntityDtoBuilder = topologyEntityDTOBuilder(
                entityType,
                TypeSpecificInfo.newBuilder()
                        .setApplicationService(ApplicationServiceInfo.newBuilder()
                                .setCurrentInstanceCount(INSTANCE_COUNT)
                                .setAppCount(APP_COUNT)
                                .build())
                        .build());
        final ApplicationServiceAspectApiDTO expected = new ApplicationServiceAspectApiDTO();
        expected.setInstanceCount(INSTANCE_COUNT);
        expected.setAppCount(APP_COUNT);
        assertApplicationServiceAspect(tpEntityDtoBuilder.build(), expected);
    }

    private void assertApplicationServiceAspect(TopologyEntityDTO tpEntityDTO,
            ApplicationServiceAspectApiDTO expected) {
        // act
        final ApplicationServiceAspectMapper mapper = new ApplicationServiceAspectMapper();
        final ApplicationServiceAspectApiDTO actual = (ApplicationServiceAspectApiDTO)mapper.mapEntityToAspect(tpEntityDTO);
        String expectedJson = GSON.toJson(expected);
        String actualJson = GSON.toJson(actual);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Test: %s\n", testName.getMethodName()));
        sb.append(String.format("TP Entity DTO: \n%s\n", tpEntityDTO));
        sb.append(String.format("Expected aspect DTO: \n%s\n", expectedJson));
        sb.append(String.format("Actual aspect DTO: \n%s", actualJson));
        logger.info(sb.toString());
        // assert
        assertThat("Expected aspect does not match actual aspect",
                actualJson, equalTo(expectedJson));
        assertEquals(AspectName.APP_SERVICE, mapper.getAspectName());
    }
}