package com.vmturbo.market.topology.conversions.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

/**
 * Unit tests for {@link ResizeInterpreter}.
 */
public class ResizeInterpreterTest {

    private CommodityConverter commodityConverter = mock(CommodityConverter.class);

    private CommodityIndex commodityIndex = mock(CommodityIndex.class);

    private Map<Long, TraderTO> oidToProjectedTraders = new HashMap<>();

    private Map<Long, TopologyEntityDTO> originalTopology = new HashMap<>();

    private Map<Long, ProjectedTopologyEntity> projectedTopology = new HashMap<>();

    private ResizeInterpreter resizeInterpreter = new ResizeInterpreter(commodityConverter,
            commodityIndex, oidToProjectedTraders, originalTopology);

    /**
     * Interpret a resize with no special cases.
     */
    @Test
    public void testInterpretNormalResize() {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .setBaseType(UICommodityType.VMEM.typeNumber())
                .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, e -> {
            e.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(topologyCommType)
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(true)
                            .setHotRemoveSupported(true)));
        }, t -> { });
        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                .setSellingTrader(entityId)
                .setOldCapacity(1)
                .setNewCapacity(2)
                .setReasonCommodity(commSpec)
                .setSpecification(commSpec)
                .setScalingGroupId("foo")
                .build();

        Resize resize = resizeInterpreter.interpret(resizeTO, projectedTopology).get();
        assertThat(resize.getCommodityType(), is(topologyCommType));
        assertThat(resize.getNewCapacity(), is(2.0f));
        assertThat(resize.getOldCapacity(), is(1.0f));
        assertThat(resize.getScalingGroupId(), is("foo"));
        assertThat(resize.getHotAddSupported(), is(true));
        assertThat(resize.getHotRemoveSupported(), is(true));
    }

    /**
     * Interpret a VM VCPU resize. Convert MhZ to cores using information on the VM.
     */
    @Test
    public void testInterpretVmVCPUResize() {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                .setType(UICommodityType.VCPU.typeNumber())
                .setBaseType(UICommodityType.VCPU.typeNumber())
                .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(UICommodityType.VCPU.typeNumber())
                .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, eBldr -> {
            eBldr.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setNumCpus(10)
                            .build())
                    .build());
        }, t -> { });
        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                .setSellingTrader(entityId)
                // Should get rounded up to 10
                .setOldCapacity(989)
                // Should get rounded up to 21 (ceiling)
                .setNewCapacity(2049)
                .setSpecification(commSpec)
                .build();

        Optional<Resize> resize = resizeInterpreter.interpret(resizeTO, projectedTopology);
        assertThat(resize.get().getCommodityType(), is(topologyCommType));
        assertThat(resize.get().getNewCapacity(), is(21.0f));
        assertThat(resize.get().getOldCapacity(), is(10.0f));
    }

    /**
     * Interpret a container VCPURequest resize. Convert MhZ to cores using information on the associated VM.
     */
    @Test
    public void testInterpretContainerVcpuResize() {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                .setType(UICommodityType.VCPU_REQUEST.typeNumber())
                .setBaseType(UICommodityType.VCPU_REQUEST.typeNumber())
                .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(UICommodityType.VCPU.typeNumber())
                .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        final long vmId = 7;
        addEntity(vmId, ApiEntityType.VIRTUAL_MACHINE, eBldr -> {
            eBldr.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setNumCpus(10)
                            .build())
                    .build());
            eBldr.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(topologyCommType)
                    // 100 mhz per CPU core
                    // 0.1 mhz per CPU millicore
                    .setCapacity(1000)
                    .build());
        }, t -> { });
        final long containerPodId = 8;
        addEntity(containerPodId, ApiEntityType.CONTAINER_POD, eBldr -> {
            eBldr.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(vmId)
                    .setProviderEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                    .build());
        }, t -> { });
        final long containerId = 9;
        addEntity(containerId, ApiEntityType.CONTAINER, eBldr -> {
            eBldr.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(containerPodId)
                    .setProviderEntityType(ApiEntityType.CONTAINER_POD.typeNumber())
                    .build());
        }, t -> { });
        when(commodityIndex.getCommSold(containerId, topologyCommType)).thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                .setSellingTrader(containerId)
                .setOldCapacity(10)
                .setNewCapacity(20)
                .setSpecification(commSpec)
                .build();

        final Optional<Resize> resize = resizeInterpreter.interpret(resizeTO, projectedTopology);
        assertThat(resize.get().getCommodityType(), is(topologyCommType));
        // 10mhz -> 20 mhz is 100 millicores to 200 millicores
        assertThat(resize.get().getOldCapacity(), is(100.0f));
        assertThat(resize.get().getNewCapacity(), is(200.0f));
    }

    /**
     * Interpret a resize as a remove limit action.
     */
    @Test
    public void testInterpretRemoveLimit() {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .setBaseType(UICommodityType.VMEM.typeNumber())
                .build();
        CommoditySoldTO m2SoldComm = CommoditySoldTO.newBuilder()
                .setSpecification(commSpec)
                .setSettings(CommoditySoldSettingsTO.newBuilder()
                        .setUtilizationUpperBound(0.5f))
                .setCapacity(100)
                .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, e -> { }, t -> t.addCommoditiesSold(m2SoldComm));
        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                .setSellingTrader(entityId)
                .setOldCapacity(1)
                .setNewCapacity(2)
                .setSpecification(commSpec)
                .build();

        Optional<Resize> resize = resizeInterpreter.interpret(resizeTO, projectedTopology);
        assertThat(resize.get().getCommodityType(), is(topologyCommType));
        assertThat(resize.get().getNewCapacity(), is(0.0f));
        assertThat(resize.get().getOldCapacity(), is(50.0f));
    }

    /**
     * Return empty under certain conditions (relevant for vSAN actions).
     */
    @Test
    public void testIgnoreIrrelevantVsanActions() {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .setBaseType(UICommodityType.VMEM.typeNumber())
                .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, e -> { }, t -> { });
        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                .setSellingTrader(entityId)
                // Sizing up
                .setOldCapacity(1)
                .setNewCapacity(2)
                .setSpecification(commSpec)
                .addResizeTriggerTrader(ResizeTriggerTraderTO.newBuilder()
                     // Not VMEM - not relevant
                    .addRelatedCommodities(UICommodityType.VCPU.typeNumber()))
                .build();

        assertThat(resizeInterpreter.interpret(resizeTO, projectedTopology), is(Optional.empty()));

        ResizeTO downSizeTO = ResizeTO.newBuilder()
                .setSellingTrader(entityId)
                .setOldCapacity(2)
                .setNewCapacity(1)
                .setSpecification(commSpec)
                .addResizeTriggerTrader(ResizeTriggerTraderTO.newBuilder()
                        // VMEM - not relevant
                        .addRelatedCommodities(UICommodityType.VMEM.typeNumber()))
                .build();
        assertThat(resizeInterpreter.interpret(downSizeTO, projectedTopology), is(Optional.empty()));
    }

    /**
     * Test explanation.
     */
    @Test
    public void testInterpretResizeExplanation() {
        ResizeTO resize = ResizeTO.newBuilder()
            .setOldCapacity(2)
            .setNewCapacity(1)
            .setScalingGroupId("foo")
            .build();
        ResizeExplanation explanation = resizeInterpreter.interpretExplanation(resize);
        assertThat(explanation.getScalingGroupId(), is("foo"));
    }

    private void addEntity(final long oid, final ApiEntityType entityType, Consumer<Builder> entityCustomizer, Consumer<TraderTO.Builder> traderCustomizer) {
        TopologyEntityDTO.Builder entityBldr = TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName(Long.toString(oid))
            .setEntityType(entityType.typeNumber());
        entityCustomizer.accept(entityBldr);
        ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
                .setEntity(entityBldr)
                .build();
        TraderTO.Builder projectedTrader = TraderTO.newBuilder()
                .setOid(oid)
                .setType(entityType.typeNumber());
        traderCustomizer.accept(projectedTrader);
        projectedTopology.put(oid, entity);
        oidToProjectedTraders.put(oid, projectedTrader.build());
        originalTopology.put(oid, entityBldr.build());
    }
}
