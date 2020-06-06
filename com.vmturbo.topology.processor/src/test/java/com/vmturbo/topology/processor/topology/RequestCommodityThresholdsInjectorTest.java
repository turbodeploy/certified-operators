package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.RequestCommodityThresholdsInjector.InjectionStats;

/**
 * RequestCommodityThresholdsInjectorTest.
 */
public class RequestCommodityThresholdsInjectorTest {

    private final RequestCommodityThresholdsInjector injector = new RequestCommodityThresholdsInjector();

    /**
     * testInjectThresholdsEmpty.
     */
    @Test
    public void testInjectThresholdsEmpty() {
        final TopologyGraph<TopologyEntity> graph = TopologyEntityUtils.topologyGraphOf();
        final InjectionStats stats = injector.injectThresholds(graph);

        assertEquals(0, stats.getEntitiesModified());
        assertEquals(0, stats.getCommoditiesModified());
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
        assertEquals(0, stats.getCommoditiesModified());
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
        assertEquals(0, stats.getCommoditiesModified());
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
        assertEquals(0, stats.getCommoditiesModified());
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
        assertEquals(1, stats.getCommoditiesModified());
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
        assertEquals(1, stats.getCommoditiesModified());
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
        assertEquals(3, stats.getCommoditiesModified());

        assertTrue(vmemRequest1.hasThresholds());
        assertEquals(vmemRequest1.getCapacity(), vmemRequest1.getThresholds().getMax(), 0);
        assertTrue(vcpuRequest.hasThresholds());
        assertEquals(vcpuRequest.getCapacity(), vcpuRequest.getThresholds().getMax(), 0);

        assertTrue(vmemRequest2.hasThresholds());
        assertEquals(vmemRequest2.getCapacity(), vmemRequest2.getThresholds().getMax(), 0);
        assertFalse(vcpu.hasThresholds());
    }

    private static CommoditySoldDTO.Builder addCommoditySold(@Nonnull final TopologyEntity.Builder entity,
                                                             final int commodityType,
                                                             final double capacity) {
        final CommoditySoldDTO.Builder commSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(commType(commodityType))
            .setCapacity(capacity);
        entity.getEntityBuilder().addCommoditySoldList(commSold);

        // Return the new builder that was added to the list
        return entity.getEntityBuilder().getCommoditySoldListBuilderList().get(
            entity.getEntityBuilder().getCommoditySoldListBuilderList().size() - 1);
    }

    private static TopologyDTO.CommodityType commType(final int type) {
        return TopologyDTO.CommodityType.newBuilder().setType(type).build();
    }
}