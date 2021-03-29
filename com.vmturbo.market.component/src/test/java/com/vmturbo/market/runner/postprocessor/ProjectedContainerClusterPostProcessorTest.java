package com.vmturbo.market.runner.postprocessor;

import static com.vmturbo.market.runner.postprocessor.ProjectedContainerClusterPostProcessor.DEFAULT_GROUP_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPlatformClusterInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.market.runner.postprocessor.ProjectedContainerClusterPostProcessor.AggregatedResourceData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test {@link ProjectedContainerClusterPostProcessor}.
 */
public class ProjectedContainerClusterPostProcessorTest {

    private final ProjectedContainerClusterPostProcessor postProcessor = new ProjectedContainerClusterPostProcessor();

    private static final long CONTAINER_CLUSTER_ID = 10L;
    private static final long VM_ID = 11L;
    private static final long CLONED_VM_ID = 21L;
    private static final long NON_CONTAINER_VM_ID = 31L;
    private static final long POD_ID = 13L;
    private static final long CLONED_POD_ID = 23L;
    private static final long CONTAINER_ID = 14L;
    private static final long CLONED_CONTAINER_ID = 24L;
    private static final long CONTAINER2_ID = 15L;

    private static final double DELTA = 0.01;
    private static final double VM_VCPU_USED = 0.5;
    private static final double VM_VCPU_CAP = 1;
    private static final double VM_VMEM_USED = 0.6;
    private static final double VM_VMEM_CAP = 2;
    private static final double VM_VCPU_REQUEST_USED = 0.7;
    private static final double VM_VCPU_REQUEST_CAP = 2;
    private static final double VM_VMEM_REQUEST_USED = 0.8;
    private static final double VM_VMEM_REQUEST_CAP = 2;
    private static final double VM_NUM_CONSUMERS_USED = 10;
    private static final double VM_NUM_CONSUMERS_CAP = 100;
    private static final double CONTAINER_VCPU_LIMIT = 1.5;
    private static final double CONTAINER_VMEM_LIMIT = 1;

    private TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyId(1234L)
        .setTopologyContextId(5678L)
        .setTopologyType(TopologyType.PLAN)
        .build();

    private final ProjectedTopologyEntity containerCluster = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE)
            .setOid(CONTAINER_CLUSTER_ID)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setContainerPlatformCluster(ContainerPlatformClusterInfo.newBuilder()
                    .setVcpuOvercommitment(1.5)
                    .setVmemOvercommitment(0.5)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setUsed(0.5)
                .setCapacity(1))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setUsed(0.5)
                .setCapacity(1))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE))
                .setUsed(0.5)
                .setCapacity(1))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                .setUsed(0.5)
                .setCapacity(1))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.NUMBER_CONSUMERS_VALUE))
                .setUsed(5)
                .setCapacity(100))
        )
        .build();

    private final ProjectedTopologyEntity vm = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(VM_ID)
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(CONTAINER_CLUSTER_ID)
                .setConnectedEntityType(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setUsed(VM_VCPU_USED)
                .setCapacity(VM_VCPU_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setUsed(VM_VMEM_USED)
                .setCapacity(VM_VMEM_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE))
                .setUsed(VM_VCPU_REQUEST_USED)
                .setCapacity(VM_VCPU_REQUEST_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                .setUsed(VM_VMEM_REQUEST_USED)
                .setCapacity(VM_VMEM_REQUEST_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.NUMBER_CONSUMERS_VALUE))
                .setUsed(VM_NUM_CONSUMERS_USED)
                .setCapacity(VM_NUM_CONSUMERS_CAP))
        )
        .build();

    private final ProjectedTopologyEntity clonedVM = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(CLONED_VM_ID)
            .setOrigin(Origin.newBuilder().setAnalysisOrigin(AnalysisOrigin.newBuilder()
                .setOriginalEntityId(VM_ID)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setUsed(VM_VCPU_USED)
                .setCapacity(VM_VCPU_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setUsed(VM_VMEM_USED)
                .setCapacity(VM_VMEM_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE))
                .setUsed(VM_VCPU_REQUEST_USED)
                .setCapacity(VM_VCPU_REQUEST_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE))
                .setUsed(VM_VMEM_REQUEST_USED)
                .setCapacity(VM_VMEM_REQUEST_CAP))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.NUMBER_CONSUMERS_VALUE))
                .setUsed(VM_NUM_CONSUMERS_USED)
                .setCapacity(VM_NUM_CONSUMERS_CAP))
        )
        .build();

    private final ProjectedTopologyEntity nonContainerVM = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(NON_CONTAINER_VM_ID)
        )
        .build();

    private final ProjectedTopologyEntity containerPod = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_POD_VALUE)
            .setOid(POD_ID)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(VM_ID))
        )
        .build();

    private final ProjectedTopologyEntity clonedContainerPod = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_POD_VALUE)
            .setOid(CLONED_POD_ID)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(CLONED_VM_ID))
        )
        .build();

    private final ProjectedTopologyEntity container = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setOid(CONTAINER_ID)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setContainer(ContainerInfo.newBuilder()
                    .setHasCpuLimit(true)
                    .setHasMemLimit(true)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setCapacity(CONTAINER_VCPU_LIMIT))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setCapacity(CONTAINER_VMEM_LIMIT))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(POD_ID))
        )
        .build();

    private final ProjectedTopologyEntity clonedContainer = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setOid(CLONED_CONTAINER_ID)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setContainer(ContainerInfo.newBuilder()
                    .setHasCpuLimit(true)
                    .setHasMemLimit(true)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setCapacity(CONTAINER_VCPU_LIMIT))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setCapacity(CONTAINER_VMEM_LIMIT))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(CLONED_POD_ID))
        )
        .build();

    // Container with false hasCpuLimit and false hasMemLimit.
    private final ProjectedTopologyEntity container2 = ProjectedTopologyEntity.newBuilder()
        .setEntity(TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setOid(CONTAINER2_ID)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setContainer(ContainerInfo.newBuilder()
                    .setHasCpuLimit(true)
                    .setHasMemLimit(false)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VCPU_VALUE))
                .setCapacity(CONTAINER_VCPU_LIMIT))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.VMEM_VALUE))
                .setCapacity(CONTAINER_VMEM_LIMIT))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(POD_ID))
        )
        .build();

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#appliesTo}.
     */
    @Test
    public void testAppliesToTrue() {
        topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1234L)
            .setTopologyContextId(5678L)
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanType(StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN))
            .build();
        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities = new HashMap<>();
        entityTypeToProjectedEntities.put(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE, Collections.emptyList());
        assertTrue(postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities));
    }

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#appliesTo}.
     */
    @Test
    public void testFalseAppliesToRealTimeTopology() {
        topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1234L)
            .setTopologyContextId(5678L)
            .setTopologyType(TopologyType.REALTIME)
            .build();
        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities = new HashMap<>();
        entityTypeToProjectedEntities.put(EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE, Collections.emptyList());
        assertFalse(postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities));
    }

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#appliesTo}.
     */
    @Test
    public void testFalseAppliesToNonContainerClusterEntity() {
        topologyInfo = TopologyInfo.newBuilder()
            .setTopologyId(1234L)
            .setTopologyContextId(5678L)
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                .setPlanType(StringConstants.OPTIMIZE_CONTAINER_CLUSTER_PLAN))
            .build();
        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToProjectedEntities = new HashMap<>();
        entityTypeToProjectedEntities.put(EntityType.VIRTUAL_MACHINE_VALUE, Collections.emptyList());
        assertFalse(postProcessor.appliesTo(topologyInfo, entityTypeToProjectedEntities));
    }

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#groupProjectedVMs}.
     */
    @Test
    public void testGetGroupedProjectedVMs() {
        final List<ProjectedTopologyEntity> vms = Arrays.asList(vm, clonedVM, nonContainerVM);
        final Map<Long, ProjectedTopologyEntity> projectedEntities = new HashMap<>();
        vms.forEach(e -> projectedEntities.put(e.getEntity().getOid(), e));
        Map<Long, List<ProjectedTopologyEntity>> groupedVMs =
            postProcessor.groupProjectedVMs("", projectedEntities, vms, new HashMap<>());
        // 3 VMs will be put into 2 groups.
        assertEquals(2, groupedVMs.size());
        assertEquals(1, groupedVMs.get(DEFAULT_GROUP_ID).size());
        assertEquals(2, groupedVMs.get(CONTAINER_CLUSTER_ID).size());
    }

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#groupProjectedContainers}.
     */
    @Test
    public void testGetGroupedProjectedContainers() {
        final List<ProjectedTopologyEntity> entities =
            Arrays.asList(container, clonedContainer, container2, containerPod, clonedContainerPod,
                vm, clonedVM);
        final Map<Long, ProjectedTopologyEntity> projectedEntities = new HashMap<>();
        entities.forEach(e -> projectedEntities.put(e.getEntity().getOid(), e));
        final List<ProjectedTopologyEntity> containers = Arrays.asList(container, clonedContainer, container2);

        Map<Long, Long> vmToClusterIdMap = new HashMap<>();
        vmToClusterIdMap.put(VM_ID, CONTAINER_CLUSTER_ID);
        vmToClusterIdMap.put(CLONED_VM_ID, CONTAINER_CLUSTER_ID);
        Map<Long, List<ProjectedTopologyEntity>> groupedContainers =
            postProcessor.groupProjectedContainers("", projectedEntities, containers, vmToClusterIdMap);
        assertEquals(1, groupedContainers.size());
        assertEquals(3, groupedContainers.get(CONTAINER_CLUSTER_ID).size());
    }

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#getAggregatedContainerResources}.
     */
    @Test
    public void testGetAggregatedContainerResources() {
        // 1. container:       VCPU limit=1.5, VMem limit=1, hasCpuLimit=true, hasMemLimit=true.
        // 2. clonedContainer: VCPU limit=1.5, VMem limit=1, hasCpuLimit=true, hasMemLimit=true.
        // 3. container2:      VCPU limit=1.5, VMem limit=1, hasCpuLimit=true, hasMemLimit=false.
        // So expected aggregated VCPU is 4.5 (1.5 + 1.5 + 1.5), and aggregated VMem is 2 (1 + 1).
        final List<ProjectedTopologyEntity> containers = Arrays.asList(container, clonedContainer, container2);
        Map<Integer, AggregatedResourceData> aggregatedData =
            postProcessor.getAggregatedContainerResources("", CONTAINER_CLUSTER_ID, containers);
        assertEquals(4.5, aggregatedData.get(CommodityDTO.CommodityType.VCPU_VALUE).getCapacity(), DELTA);
        assertEquals(2, aggregatedData.get(CommodityDTO.CommodityType.VMEM_VALUE).getCapacity(), DELTA);
    }

    /**
     * Test {@link ProjectedContainerClusterPostProcessor#process}.
     */
    @Test
    public void testProcess() {
        // 1. container:  VCPU limit=1.5, VMem limit=1, hasCpuLimit=true, hasMemLimit=true.
        // 2. container2: VCPU limit=1.5, VMem limit=1, hasCpuLimit=true, hasMemLimit=false.
        // 3. vm:         VCPU cap=1, VMem cap=2
        //
        // So
        // expected vcpuOvercommitment = (1.5 + 1.5) / 1 = 3.0
        // expected vmemOvercommitment = 1 / 2 = 0.5
        final List<ProjectedTopologyEntity> entities =
            Arrays.asList(containerCluster, container, container2, containerPod, vm);
        Map<Long, ProjectedTopologyEntity> projectedEntities = new HashMap<>();
        entities.forEach(e -> projectedEntities.put(e.getEntity().getOid(), e));

        Map<Integer, List<ProjectedTopologyEntity>> entityTypeToEntitiesMap =
            getTypeToEntitiesMap(projectedEntities);
        postProcessor.process(topologyInfo, projectedEntities, entityTypeToEntitiesMap, Collections.emptyList());
        ProjectedTopologyEntity updatedContainerCluster = projectedEntities.get(CONTAINER_CLUSTER_ID);
        ContainerPlatformClusterInfo containerPlatformClusterInfo =
            updatedContainerCluster.getEntity().getTypeSpecificInfo().getContainerPlatformCluster();
        assertEquals(3.0, containerPlatformClusterInfo.getVcpuOvercommitment(), DELTA);
        assertEquals(0.5, containerPlatformClusterInfo.getVmemOvercommitment(), DELTA);

        Map<Integer, Double> expectedCommUsedMap = new HashMap<Integer, Double>() {{
            put(CommodityDTO.CommodityType.VCPU_VALUE, VM_VCPU_USED);
            put(CommodityDTO.CommodityType.VMEM_VALUE, VM_VMEM_USED);
            put(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE, VM_VCPU_REQUEST_USED);
            put(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE, VM_VMEM_REQUEST_USED);
            put(CommodityDTO.CommodityType.NUMBER_CONSUMERS_VALUE, VM_NUM_CONSUMERS_USED);
        }};
        Map<Integer, Double> expectedCommCapMap = new HashMap<Integer, Double>() {{
            put(CommodityDTO.CommodityType.VCPU_VALUE, VM_VCPU_CAP);
            put(CommodityDTO.CommodityType.VMEM_VALUE, VM_VMEM_CAP);
            put(CommodityDTO.CommodityType.VCPU_REQUEST_VALUE, VM_VCPU_REQUEST_CAP);
            put(CommodityDTO.CommodityType.VMEM_REQUEST_VALUE, VM_VMEM_REQUEST_CAP);
            put(CommodityDTO.CommodityType.NUMBER_CONSUMERS_VALUE, VM_NUM_CONSUMERS_CAP);
        }};
        for (CommoditySoldDTO commSold : updatedContainerCluster.getEntity().getCommoditySoldListList()) {
            assertEquals(String.format("Used value of %s doesn't match", CommodityDTO.CommodityType.forNumber(commSold.getCommodityType().getType())),
                expectedCommUsedMap.get(commSold.getCommodityType().getType()), commSold.getUsed(), DELTA);
            assertEquals(String.format("Capacity value of %s doesn't match", CommodityDTO.CommodityType.forNumber(commSold.getCommodityType().getType())),
                expectedCommCapMap.get(commSold.getCommodityType().getType()), commSold.getCapacity(), DELTA);
        }
    }

    private Map<Integer, List<ProjectedTopologyEntity>> getTypeToEntitiesMap(
        @Nonnull final Map<Long, ProjectedTopologyEntity> projectedEntities) {
         return projectedEntities.values().stream()
                .collect(Collectors.groupingBy(entity -> entity.getEntity().getEntityType()));
    }
}