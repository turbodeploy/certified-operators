package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.WorkloadControllerAspectApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CronJobInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CustomControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DeploymentInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo.ControllerTypeCase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * RegionAspectMapperTest test class for Region aspect mapper.
 */
public class WorkloadControllerAspectMapperTest extends BaseAspectMapperTest {

    private static final long DEPLOYMENT_OID = 1001L;
    private static final long WORKLOAD_CONTROLLER_OID_2 = 1002L;
    private static final long WORKLOAD_CONTROLLER_OID_3 = 1003L;
    private static final String FOO_CUSTOM_CONTROLLER_TYPE = "Foo";
    private TopologyEntityDTO deploymentTopologyEntityDTO;
    private TopologyEntityDTO cronJobTopologyEntityDTO;
    private TopologyEntityDTO customControllerTopologyEntityDTO;

    /**
     * Set up test and create a topology entity dto.
     **/
    @Before
    public void setUp() {
        deploymentTopologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
                .setOid(DEPLOYMENT_OID)
                .setTypeSpecificInfo(
                    TypeSpecificInfo.newBuilder()
                        .setWorkloadController(WorkloadControllerInfo.newBuilder()
                                .setDeploymentInfo(DeploymentInfo.getDefaultInstance())))
            .build();

        cronJobTopologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
            .setOid(WORKLOAD_CONTROLLER_OID_2)
            .setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setCronJobInfo(CronJobInfo.getDefaultInstance())))
                .build();

        customControllerTopologyEntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.WORKLOAD_CONTROLLER_VALUE)
            .setOid(WORKLOAD_CONTROLLER_OID_3)
            .setTypeSpecificInfo(
                TypeSpecificInfo.newBuilder()
                    .setWorkloadController(WorkloadControllerInfo.newBuilder()
                        .setCustomControllerInfo(CustomControllerInfo.newBuilder()
                        .setCustomControllerType(FOO_CUSTOM_CONTROLLER_TYPE))))
            .build();
    }

    /**
     * Map entity to its aspect and verify if the information is correct.
     * @throws InterruptedException if interrupted.
     * @throws ConversionException if something goes wrong during conversion.
     **/
    @Test
    public void testMapEntityToAspect() throws InterruptedException, ConversionException {
        WorkloadControllerAspectMapper testMapper = new WorkloadControllerAspectMapper();

        // act
        final Optional<Map<Long, EntityAspect>> resultAspect1 =
            testMapper.mapEntityToAspectBatch(Collections.singletonList(deploymentTopologyEntityDTO));

        // assert
        final WorkloadControllerAspectApiDTO deployment =
            (WorkloadControllerAspectApiDTO)resultAspect1.get().get(DEPLOYMENT_OID);
        assertNotNull(deployment);
        assertEquals(ControllerTypeCase.DEPLOYMENT_INFO.name(), deployment.getControllerType());
        assertNull(deployment.getCustomControllerType());

        // act
        final WorkloadControllerAspectApiDTO cronJob = testMapper.mapEntityToAspect(cronJobTopologyEntityDTO);

        // assert
        assertNotNull(cronJob);
        assertEquals(ControllerTypeCase.CRON_JOB_INFO.name(), cronJob.getControllerType());
        assertNull(cronJob.getCustomControllerType());

        // act
        final WorkloadControllerAspectApiDTO customController = testMapper.mapEntityToAspect(customControllerTopologyEntityDTO);

        // assert
        assertNotNull(customController);
        assertEquals(ControllerTypeCase.CUSTOM_CONTROLLER_INFO.name(), customController.getControllerType());
        assertEquals(FOO_CUSTOM_CONTROLLER_TYPE, customController.getCustomControllerType());
    }
}
