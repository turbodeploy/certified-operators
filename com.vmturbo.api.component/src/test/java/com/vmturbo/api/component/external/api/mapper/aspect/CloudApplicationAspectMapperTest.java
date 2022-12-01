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

import com.vmturbo.api.dto.entityaspect.CloudApplicationAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudApplicationInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test mapping of entity information for Cloud Application Aspect.
 */
public class CloudApplicationAspectMapperTest extends BaseAspectMapperTest {

    /**
     * name of test.
     */
    @Rule
    public TestName testName = new TestName();

    private static final Logger logger = LogManager.getLogger();

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final Integer HYBRID_CONNECITON_COUNT = 30;
    private static final Integer DEPLOYMENT_SLOT_COUNT = 10;

    /**
     * Test standard usage of mapping an entity to app component aspect.
     */
    @Test
    public void testMapEntityToAspect() {
        testMapEntityToAspect(EntityType.APPLICATION_COMPONENT_SPEC);
    }

    private void testMapEntityToAspect(EntityType entityType) {
        // arrange
        final TopologyEntityDTO.Builder tpEntityDtoBuilder = topologyEntityDTOBuilder(entityType, TypeSpecificInfo.newBuilder()
                        .setCloudApplication(CloudApplicationInfo.newBuilder()
                                .setDeploymentSlotCount(DEPLOYMENT_SLOT_COUNT)
                                .setHybridConnectionCount(HYBRID_CONNECITON_COUNT)
                                .build())
                        .build());
        final CloudApplicationAspectApiDTO expected = new CloudApplicationAspectApiDTO();
        expected.setDeploymentSlotCount(DEPLOYMENT_SLOT_COUNT);
        expected.setHybridConnectionCount(HYBRID_CONNECITON_COUNT);
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
                        .setCloudApplication(CloudApplicationInfo.newBuilder()
                                .setHybridConnectionCount(HYBRID_CONNECITON_COUNT)
                                .build())
                        .build());
        final CloudApplicationAspectApiDTO expected = new CloudApplicationAspectApiDTO();
        expected.setHybridConnectionCount(HYBRID_CONNECITON_COUNT);
        assertApplicationServiceAspect(tpEntityDtoBuilder.build(), expected);
    }

    private void assertApplicationServiceAspect(TopologyEntityDTO tpEntityDTO,
            CloudApplicationAspectApiDTO expected) {
        // act
        final CloudApplicationAspectMapper mapper = new CloudApplicationAspectMapper();
        final CloudApplicationAspectApiDTO actual = (CloudApplicationAspectApiDTO)mapper.mapEntityToAspect(tpEntityDTO);
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
        assertEquals(AspectName.CLOUD_APPLICATION, mapper.getAspectName());
    }
}
