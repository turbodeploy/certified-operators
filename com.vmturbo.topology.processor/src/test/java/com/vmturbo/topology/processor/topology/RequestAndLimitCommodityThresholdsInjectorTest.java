package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.RequestAndLimitCommodityThresholdsInjector.InjectionStats;

/**
 * RequestCommodityThresholdsInjectorTest.
 */
public class RequestAndLimitCommodityThresholdsInjectorTest {

    private final RequestAndLimitCommodityThresholdsInjector injector = new RequestAndLimitCommodityThresholdsInjector();

    /**
     * testInjectThresholdsEmpty.
     */
    @Test
    public void testInjectThresholdsEmpty() {
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf();
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getRequestCommoditiesModified());
    }

    /**
     * testInjectThresholdsNoContainers.
     */
    @Test
    public void testInjectThresholdsNoContainers() {
        final TopologyEntity.Builder stEntity = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "ST", EntityType.STORAGE);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(stEntity);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getRequestCommoditiesModified());
    }

    /**
     * testInjectThresholdsNoCommodities.
     */
    @Test
    public void testInjectThresholdsNoCommodities() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getRequestCommoditiesModified());
    }

    /**
     * testInjectThresholdsNoRequestCommodities.
     */
    @Test
    public void testInjectThresholdsNoRequestCommodities() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vcpu = addCommoditySold(container, CommodityType.VCPU_VALUE, 50.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getRequestCommoditiesModified());
        assertFalse(vcpu.hasThresholds());
    }

    /**
     * testInjectThresholdsVcpuRequest.
     */
    @Test
    public void testInjectThresholdsVcpuRequest() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vcpuRequest =
            addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 50.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(1, stats.getEntitiesModified());
        assertEquals(1, stats.getRequestCommoditiesModified());
        assertTrue(vcpuRequest.hasThresholds());
        assertEquals(50.0, vcpuRequest.getThresholds().getMax(), 0);
    }

    /**
     * testInjectThresholdsVMemRequest.
     */
    @Test
    public void testInjectThresholdsVMemRequest() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vmemRequest =
            addCommoditySold(container, CommodityType.VMEM_REQUEST_VALUE, 50.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(1, stats.getEntitiesModified());
        assertEquals(1, stats.getRequestCommoditiesModified());
        assertTrue(vmemRequest.hasThresholds());
        assertEquals(50.0, vmemRequest.getThresholds().getMax(), 0);
    }

    /**
     * testInjectThresholdsMultipleContainers.
     */
    @Test
    public void testInjectThresholdsMultipleContainers() {
        final TopologyEntity.Builder container1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container1", EntityType.CONTAINER);
        final TopologyEntity.Builder container2 = TopologyEntityUtils
            .topologyEntity(1, 0, 0, "Container2", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vmemRequest1 =
            addCommoditySold(container1, CommodityType.VMEM_REQUEST_VALUE, 50.0);
        final CommoditySoldDTO.Builder vcpuRequest =
            addCommoditySold(container1, CommodityType.VCPU_REQUEST_VALUE, 20.0);
        final CommoditySoldDTO.Builder vmemRequest2 =
            addCommoditySold(container2, CommodityType.VMEM_REQUEST_VALUE, 510.0);
        final CommoditySoldDTO.Builder vcpu =
            addCommoditySold(container2, CommodityType.VCPU_VALUE, 5.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container1, container2);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(2, stats.getEntitiesModified());
        assertEquals(3, stats.getRequestCommoditiesModified());

        assertTrue(vmemRequest1.hasThresholds());
        assertEquals(vmemRequest1.getCapacity(), vmemRequest1.getThresholds().getMax(), 0);
        assertTrue(vcpuRequest.hasThresholds());
        assertEquals(vcpuRequest.getCapacity(), vcpuRequest.getThresholds().getMax(), 0);

        assertTrue(vmemRequest2.hasThresholds());
        assertEquals(vmemRequest2.getCapacity(), vmemRequest2.getThresholds().getMax(), 0);
        assertFalse(vcpu.hasThresholds());
    }

    /**
     * testInjectVMemLimitThresholds.
     */
    @Test
    public void testInjectVMemLimitThresholds() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        addCommoditySold(container, CommodityType.VMEM_REQUEST_VALUE, 50.0);
        final CommoditySoldDTO.Builder vmemLimit =
            addCommoditySold(container, CommodityType.VMEM_VALUE, 150.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(1, stats.getEntitiesModified());
        assertEquals(1, stats.getRequestCommoditiesModified());
        assertEquals(1, stats.getLimitCommoditiesModified());
        assertTrue(vmemLimit.hasThresholds());
        assertEquals(50.0, vmemLimit.getThresholds().getMin(), 0);
    }

    /**
     * testInjectVcpuLimitThresholds.
     */
    @Test
    public void testInjectVcpuLimitThresholds() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 50.0);
        final CommoditySoldDTO.Builder vcpuLimit =
            addCommoditySold(container, CommodityType.VCPU_VALUE, 150.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(1, stats.getEntitiesModified());
        assertEquals(1, stats.getRequestCommoditiesModified());
        assertEquals(1, stats.getLimitCommoditiesModified());
        assertTrue(vcpuLimit.hasThresholds());
        assertEquals(50.0, vcpuLimit.getThresholds().getMin(), 0);
    }

    /**
     * testInjectVcpuLimitWithoutRequest.
     */
    @Test
    public void testInjectVcpuLimitWithoutRequest() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vcpuLimit =
            addCommoditySold(container, CommodityType.VCPU_VALUE, 150.0);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getRequestCommoditiesModified());
        assertEquals(0, stats.getLimitCommoditiesModified());
        assertFalse(vcpuLimit.hasThresholds());
    }

    /**
     * testInjectVcpuLimitWithExistingThresholds.
     * <p/>
     * If there's an existing max threshold for request commodity, update max threshold as the min of
     * existing max threshold and request capacity;
     * If there's an existing min threshold for limit commodity, update min threshold as the max of
     * existing min threshold and request capacity.
     */
    @Test
    public void testInjectVcpuLimitWithExistingThresholds() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vCPUComm =
            addCommoditySold(container, CommodityType.VCPU_VALUE, 100.0, 20.0, true);
        vCPUComm.setThresholds(Thresholds.newBuilder().setMin(60).build());

        final CommoditySoldDTO.Builder vCPURequestComm =
            addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 50.0, 20.0, true);
        vCPURequestComm.setThresholds(Thresholds.newBuilder().setMax(60).build());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(1, stats.getEntitiesModified());
        assertEquals(1, stats.getRequestCommoditiesModified());
        assertEquals(1, stats.getLimitCommoditiesModified());
        assertEquals(60, vCPUComm.getThresholds().getMin(), 0);
        assertEquals(50, vCPURequestComm.getThresholds().getMax(), 0);
    }

    /**
     * testLimitNotResizable.
     */
    @Test
    public void testLimitNotResizable() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 50.0);
        final CommoditySoldDTO.Builder vcpuLimit =
            addCommoditySold(container, CommodityType.VCPU_VALUE, 150.0, false);

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container);
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(1, stats.getEntitiesModified());
        assertEquals(1, stats.getRequestCommoditiesModified());
        assertEquals(0, stats.getLimitCommoditiesModified());
        assertFalse(vcpuLimit.hasThresholds());
    }

    /**
     * testInjectMinThresholdsFromUsageEmpty.
     */
    @Test
    public void testInjectMinThresholdsFromUsageEmpty() {
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf();
        final InjectionStats stats = injector.injectMinThresholdsFromUsage(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
    }

    /**
     * testInjectMinThresholdsFromUsageNoContainerSpecs.
     */
    @Test
    public void testInjectMinThresholdsFromUsageNoContainerSpecs() {
        final TopologyEntity.Builder stEntity = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "ST", EntityType.STORAGE);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(stEntity);
        final InjectionStats stats = injector.injectMinThresholdsFromUsage(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
    }

    /**
     * testInjectMinThresholdsFromUsageNoCommodities.
     */
    @Test
    public void testInjectMinThresholdsFromUsageNoCommodities() {
        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final TopologyEntity.Builder containerSpec = TopologyEntityUtils
            .topologyEntity(1, 0, 0, "ContainerSpec", EntityType.CONTAINER_SPEC)
            .addControlledEntity(container);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container, containerSpec);
        final InjectionStats stats = injector.injectMinThresholdsFromUsage(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
    }


    /**
     * testInjectMinThresholdsFromUsageMultipleContainers.
     */
    @Test
    public void testInjectMinThresholdsFromUsageMultipleContainers() {
        final TopologyEntity.Builder containerSpec = TopologyEntityUtils
            .topologyEntity(2, 0, 0, "ContainerSpec", EntityType.CONTAINER_SPEC);

        final TopologyEntity.Builder container1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container1", EntityType.CONTAINER);
        final TopologyEntity.Builder container2 = TopologyEntityUtils
            .topologyEntity(1, 0, 0, "Container2", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder vcpu1 =
            addCommoditySold(container1, CommodityType.VCPU_VALUE, 50.0, 10.0, true);
        final CommoditySoldDTO.Builder vcpu2 =
            addCommoditySold(container2, CommodityType.VCPU_VALUE, 50.0, 20.0, true);

        addControlledEntity(container1, containerSpec.getOid());
        addControlledEntity(container2, containerSpec.getOid());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container1, container2, containerSpec);
        final InjectionStats stats = injector.injectMinThresholdsFromUsage(graph);

        assertEquals(2, stats.getEntitiesModified());
        assertEquals(2, stats.getCommoditiesModified());

        assertTrue(vcpu1.hasThresholds());
        assertEquals(vcpu2.getUsed(), vcpu1.getThresholds().getMin(), 0);

        assertTrue(vcpu2.hasThresholds());
        assertEquals(vcpu2.getUsed(), vcpu2.getThresholds().getMin(), 0);
    }

    /**
     * testInjectMinThresholdsFromUsageWithExistingThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromUsageWithExistingThresholds() {
        final TopologyEntity.Builder containerSpec = TopologyEntityUtils
            .topologyEntity(2, 0, 0, "ContainerSpec", EntityType.CONTAINER_SPEC);

        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container1", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 50.0, 20.0, true);
        comm.setThresholds(Thresholds.newBuilder().setMin(10).setMax(100).build());

        addControlledEntity(container, containerSpec.getOid());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container, containerSpec);
        injector.injectMinThresholdsFromUsage(graph);

        // commodity min threshold is updated to 20 and max threshold is still 100.
        assertTrue(comm.hasThresholds());
        assertEquals(20, comm.getThresholds().getMin(), 0);
        assertEquals(100, comm.getThresholds().getMax(), 0);
    }

    /**
     * testInjectMinThresholdsFromUsageWithOnlyExistingMinThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromUsageWithOnlyExistingMinThresholds() {
        final TopologyEntity.Builder containerSpec = TopologyEntityUtils
            .topologyEntity(2, 0, 0, "ContainerSpec", EntityType.CONTAINER_SPEC);

        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container1", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 50.0, 20.0, true);
        comm.setThresholds(Thresholds.newBuilder().setMin(10).build());

        addControlledEntity(container, containerSpec.getOid());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container, containerSpec);
        injector.injectMinThresholdsFromUsage(graph);

        // commodity min threshold is updated to 20 and max threshold is still 100.
        assertTrue(comm.hasThresholds());
        assertEquals(20, comm.getThresholds().getMin(), 0);
        // commodity has no max thresholds.
        assertFalse(comm.getThresholds().hasMax());
    }

    /**
     * testInjectMinThresholdsFromUsageWithUsageLargerThanMaxThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromUsageWithUsageLargerThanMaxThresholds() {
        final TopologyEntity.Builder containerSpec = TopologyEntityUtils
            .topologyEntity(2, 0, 0, "ContainerSpec", EntityType.CONTAINER_SPEC);

        final TopologyEntity.Builder container = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "Container", EntityType.CONTAINER);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(container, CommodityType.VCPU_REQUEST_VALUE, 80.0, 100.0, true);
        comm.setThresholds(Thresholds.newBuilder().setMax(80).build());

        addControlledEntity(container, containerSpec.getOid());

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(container, containerSpec);
        injector.injectMinThresholdsFromUsage(graph);

        // commodity min threshold is capped to max threshold 80 with current usage 100.
        assertTrue(comm.hasThresholds());
        assertEquals(80, comm.getThresholds().getMin(), 0);
        assertEquals(80, comm.getThresholds().getMax(), 0);
    }

    /**
     * injectMinThresholdsFromReservationEmpty.
     */
    @Test
    public void injectMinThresholdsFromReservationEmpty() {
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf();
        final InjectionStats stats = injector.injectMinThresholdsFromReservation(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
    }

    /**
     * injectMinThresholdsFromReservationNoVM.
     */
    @Test
    public void injectMinThresholdsFromReservationNoVM() {
        final TopologyEntity.Builder stEntity = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "ST", EntityType.STORAGE);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(stEntity);
        final InjectionStats stats = injector.injectMinThresholdsFromReservation(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
    }

    /**
     * injectMinThresholdsFromReservationNoCommodities.
     */
    @Test
    public void injectMinThresholdsFromReservationNoCommodities() {
        final TopologyEntity.Builder vm1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "VM1", EntityType.VIRTUAL_MACHINE);
        final TopologyEntity.Builder vm2 = TopologyEntityUtils
            .topologyEntity(1, 0, 0, "VM2", EntityType.VIRTUAL_MACHINE);
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(vm1, vm2);
        final InjectionStats stats = injector.injectMinThresholdsFromReservation(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
    }

    /**
     * testInjectMinThresholdsFromReservationNoThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromReservationNoThresholds() {
        final TopologyEntity.Builder vm1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "VM1", EntityType.VIRTUAL_MACHINE);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(vm1, CommodityType.VCPU_VALUE, 50.0, 20.0, true);

        final CommodityBoughtDTO commBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CPU_VALUE))
            .setReservedCapacity(20).build();
        vm1.getEntityBuilder().addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommodityBought(commBought));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(vm1);
        injector.injectMinThresholdsFromReservation(graph);

        // commodity min threshold is updated to 20 and max threshold is still 100.
        assertTrue(comm.hasThresholds());
        assertEquals(20, comm.getThresholds().getMin(), 0);
        // commodity has no max thresholds.
        assertFalse(comm.getThresholds().hasMax());
    }

    /**
     * testInjectMinThresholdsFromReservationWithExistingThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromReservationWithExistingThresholds() {
        final TopologyEntity.Builder vm1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "VM1", EntityType.VIRTUAL_MACHINE);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(vm1, CommodityType.VCPU_VALUE, 50.0, 20.0, true);
        comm.setThresholds(Thresholds.newBuilder().setMin(10).setMax(100).build());

        final CommodityBoughtDTO commBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CPU_VALUE))
            .setReservedCapacity(20).build();
        vm1.getEntityBuilder().addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommodityBought(commBought));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(vm1);
        injector.injectMinThresholdsFromReservation(graph);

        // commodity min threshold is updated to 20 and max threshold is still 100.
        assertTrue(comm.hasThresholds());
        assertEquals(20, comm.getThresholds().getMin(), 0);
        assertEquals(100, comm.getThresholds().getMax(), 0);
    }

    /**
     * testInjectMinThresholdsFromReservationWithOnlyExistingMinThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromReservationWithReservationSmallerThanMinThresholds() {
        final TopologyEntity.Builder vm1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "VM1", EntityType.VIRTUAL_MACHINE);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(vm1, CommodityType.VCPU_VALUE, 50.0, 20.0, true);
        comm.setThresholds(Thresholds.newBuilder().setMin(10).build());

        final CommodityBoughtDTO commBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CPU_VALUE))
            .setReservedCapacity(5).build();
        vm1.getEntityBuilder().addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommodityBought(commBought));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(vm1);
        injector.injectMinThresholdsFromReservation(graph);

        // commodity min threshold is not updated.
        assertTrue(comm.hasThresholds());
        assertEquals(10, comm.getThresholds().getMin(), 0);
        // commodity has no max thresholds.
        assertFalse(comm.getThresholds().hasMax());
    }

    /**
     * testInjectMinThresholdsFromReservationWithReservationLargerThanMaxThresholdsAndNoMinThresholds.
     */
    @Test
    public void testInjectMinThresholdsFromReservationWithReservationLargerThanMaxThresholdsAndNoMinThresholds() {
        final TopologyEntity.Builder vm1 = TopologyEntityUtils
            .topologyEntity(0, 0, 0, "VM1", EntityType.VIRTUAL_MACHINE);
        final CommoditySoldDTO.Builder comm =
            addCommoditySold(vm1, CommodityType.VCPU_VALUE, 50.0, 20.0, true);
        comm.setThresholds(Thresholds.newBuilder().setMax(80).build());

        final CommodityBoughtDTO commBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CPU_VALUE))
            .setReservedCapacity(100).build();
        vm1.getEntityBuilder().addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .addCommodityBought(commBought));

        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf(vm1);
        injector.injectMinThresholdsFromReservation(graph);

        // commodity min threshold is capped to max threshold 80 with current reservation 100.
        assertTrue(comm.hasThresholds());
        assertEquals(80, comm.getThresholds().getMin(), 0);
        assertEquals(80, comm.getThresholds().getMax(), 0);
    }

    private static CommoditySoldDTO.Builder addCommoditySold(@Nonnull final TopologyEntity.Builder entity,
                                                             final int commodityType,
                                                             final double capacity) {
        return addCommoditySold(entity, commodityType, capacity, true);
    }

    private static CommoditySoldDTO.Builder addCommoditySold(@Nonnull final TopologyEntity.Builder entity,
                                                             final int commodityType,
                                                             final double capacity,
                                                             final boolean resizable) {
        return addCommoditySold(entity, commodityType, capacity, 0.0, resizable);
    }

    private static CommoditySoldDTO.Builder addCommoditySold(@Nonnull final TopologyEntity.Builder entity,
                                                             final int commodityType,
                                                             final double capacity,
                                                             final double used,
                                                             final boolean resizable) {
        final CommoditySoldDTO.Builder commSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(commType(commodityType))
            .setCapacity(capacity)
            .setUsed(used)
            .setIsResizeable(resizable);
        entity.getEntityBuilder().addCommoditySoldList(commSold);

        // Return the new builder that was added to the list
        return entity.getEntityBuilder().getCommoditySoldListBuilderList().get(
            entity.getEntityBuilder().getCommoditySoldListBuilderList().size() - 1);
    }

    private static TopologyDTO.CommodityType commType(final int type) {
        return TopologyDTO.CommodityType.newBuilder().setType(type).build();
    }

    private static void addControlledEntity(@Nonnull final TopologyEntity.Builder entity, long connectedEntityId) {
        entity.getEntityBuilder()
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(connectedEntityId)
                .setConnectionType(ConnectionType.CONTROLLED_BY_CONNECTION)
                .build());
    }
}