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
        assertVCPUCapacity(10, 1.0d, 989.0f, 2049.0f, 21.0f);
        assertVCPUCapacity(4, 1.0000279520190538d, 10372.29f, 15558.436f, 6.0f);
        assertVCPUCapacity(8, 1.0489584084750498d, 18461.668f, 23077.086f, 10.0f);
        assertVCPUCapacity(1, 3.5859d, 8606.16f, 51638.83698998277f, 6.0f);
        assertVCPUCapacity(1, 3.58593d, 8606.232f, 51638.83698998277f, 6.0f);
    }

    /**
     * Interpret a VM VCPU resize. Convert MhZ to cores using information on the VM.
     *
     * @param numCpus numCpus
     * @param scalingFactor scalingFactor
     * @param oldCapacity oldCapacity
     * @param newCapacity newCapacity
     * @param newCores newCores
     */
    private void assertVCPUCapacity(int numCpus, double scalingFactor, float oldCapacity, float newCapacity, float newCores) {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
            .setType(UICommodityType.VCPU.typeNumber())
            .setBaseType(UICommodityType.VCPU.typeNumber())
            .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
            .setType(UICommodityType.VCPU.typeNumber())
            .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, eBldr ->
            eBldr.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus(numCpus).build())
                .build()), t -> { });
        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.of(
            CommoditySoldDTO.newBuilder().setCommodityType(CommodityType.newBuilder().setType(123))
                .setScalingFactor(scalingFactor).build()));
        ResizeTO resizeTO = ResizeTO.newBuilder()
            .setSellingTrader(entityId)
            .setOldCapacity(oldCapacity)
            .setNewCapacity(newCapacity)
            .setSpecification(commSpec)
            .build();

        Optional<Resize> resize = resizeInterpreter.interpret(resizeTO, projectedTopology);
        assertThat(resize.get().getCommodityType(), is(topologyCommType));
        assertThat(resize.get().getOldCapacity(), is((float)numCpus));
        assertThat(resize.get().getNewCapacity(), is(newCores));
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
