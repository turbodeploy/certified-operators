package com.vmturbo.market.runner.postprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link ProjectedContainerSpecPostProcessor}.
 */
public class ProjectedContainerSpecPostProcessorTest {

    private final ProjectedContainerSpecPostProcessor postProcessor = new ProjectedContainerSpecPostProcessor();
    private static final double DELTA = 0.01;
    private TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyId(1234L)
        .setTopologyContextId(5678L)
        .setTopologyType(TopologyType.REALTIME)
        .build();

    /**
     * Test {@link ProjectedContainerSpecPostProcessor#appliesTo}.
     */
    @Test
    public void testAppliesToTrue() {
        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities = new HashMap<>();
        entityTypeToProjectedEntities.put(EntityType.CONTAINER_SPEC_VALUE, Collections.emptyList());
        assertTrue(postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities));
    }

    /**
     * Test {@link ProjectedContainerSpecPostProcessor#appliesTo}.
     */
    @Test
    public void testFalseAppliesToPlanTopology() {
        topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1234L)
            .setTopologyContextId(5678L)
            .setTopologyType(TopologyType.PLAN)
            .build();
        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities = new HashMap<>();
        entityTypeToProjectedEntities.put(EntityType.VIRTUAL_MACHINE_VALUE, Collections.emptyList());
        assertFalse(postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities));
    }

    /**
     * Test {@link ProjectedContainerSpecPostProcessor#appliesTo}.
     */
    @Test
    public void testFalseAppliesToNonContainerSpecEntity() {
        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities = new HashMap<>();
        entityTypeToProjectedEntities.put(EntityType.VIRTUAL_MACHINE_VALUE, Collections.emptyList());
        assertFalse(postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities));
    }

    /**
     * Test {@link ProjectedContainerSpecPostProcessor#process}.
     */
    @Test
    public void testProjectedContainerSpecsPostProcessing() {
        // Container1 and Container2 are connected to the same ContainerSpec.
        // Resize actions are generated on 2 containers and projectedContainerSpecsPostProcessing
        // will update ContainerSpec commodity capacity and percentile utilization from projected
        // Container entities.
        long containerSpecOID = 11L;
        final ProjectedTopologyEntity containerSpec = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_SPEC_VALUE)
                .setOid(containerSpecOID)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                        .build())
                    .setCapacity(1)
                    .setHistoricalUsed(HistoricalValues.newBuilder()
                        .setPercentile(10)
                        .build())
                    .build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE)
                                .build())
                        .setCapacity(100)
                        .setUsed(50)
                        .build())
                .build())
            .build();
        long containerOID1 = 22L;
        final ProjectedTopologyEntity container1 = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(containerOID1)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                        .build())
                    .setCapacity(4)
                    .build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE)
                                .build())
                        .setCapacity(100)
                        .setUsed(20)
                        .build())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(containerSpecOID)
                    .build())
                .build())
            .build();
        long containerOID2 = 33L;
        final ProjectedTopologyEntity container2 = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(containerOID2)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                        .build())
                    .setCapacity(2)
                    .build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.VCPU_THROTTLING_VALUE)
                                .build())
                        .setCapacity(100)
                        .setUsed(10)
                        .build())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectedEntityId(containerSpecOID)
                    .build())
                .build())
            .build();
        final Map<Long, ProjectedTopologyEntity> projectedTopologyEntityMap = new HashMap<>();
        projectedTopologyEntityMap.put(containerSpecOID, containerSpec);
        projectedTopologyEntityMap.put(containerOID1, container1);
        projectedTopologyEntityMap.put(containerOID2, container2);

        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities =
            projectedTopologyEntityMap.values().stream()
                .collect(Collectors.groupingBy(entity -> entity.getEntity().getEntityType()));

        ActionTO actionTO1 = mockActionTO(containerOID1, CommodityDTO.CommodityType.VCPU_VALUE);
        ActionTO actionTO2 = mockActionTO(containerOID2, CommodityDTO.CommodityType.VCPU_VALUE);
        List<ActionTO> actionTOList = Arrays.asList(actionTO1, actionTO2);

        postProcessor.process(topologyInfo, projectedTopologyEntityMap, entityTypeToProjectedEntities, actionTOList);

        ProjectedTopologyEntity updatedProjectedContainerSpec =
            projectedTopologyEntityMap.get(containerSpecOID);
        Assert.assertNotNull(updatedProjectedContainerSpec);
        // Updated VCPU capacity value of container spec is the max capacity value of all corresponding
        // container replicas.
        Assert.assertEquals(4,
            updatedProjectedContainerSpec.getEntity().getCommoditySoldList(0).getCapacity(), DELTA);
        Assert.assertEquals(2.5,
            updatedProjectedContainerSpec.getEntity().getCommoditySoldList(0).getHistoricalUsed().getPercentile(), DELTA);
        // Updated VCPUThrottling used value of container spec is the average used value of all corresponding
        // container replicas.
        Assert.assertEquals(15,
                updatedProjectedContainerSpec.getEntity().getCommoditySoldList(1).getUsed(), DELTA);
    }

    private ActionTO mockActionTO(long entityOID, int commodityType) {
        return ActionTO.newBuilder()
            .setResize(ResizeTO.newBuilder()
                .setSellingTrader(entityOID)
                .setSpecification(CommoditySpecificationTO.newBuilder()
                    .setType(1)
                    .setBaseType(commodityType)
                    .build())
                .build())
            .setImportance(100)
            .setIsNotExecutable(false)
            .build();
    }
}