package com.vmturbo.market.topology.conversions.action;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo.CpuScalingPolicy;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTriggerTraderTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
     * Checks that resize with CPS will be generated in case CPS marked as changeable for entity.
     */
    @Test
    public void testResizeWithCpsForCpsChangeable() {
        checkResizeWithCps(true);
    }

    /**
     * Checks that resize with CPS will not be generated in case CPS marked as unchangeable for
     * entity.
     */
    @Test
    public void testResizeWithCpsForCpsUnchangeable() {
        checkResizeWithCps(false);
    }

    private void checkResizeWithCps(boolean cpsChangeable) {
        final CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                        .setType(UICommodityType.VCPU.typeNumber())
                        .setBaseType(UICommodityType.VCPU.typeNumber())
                        .build();
        final CommodityType topologyCommType = CommodityType.newBuilder()
                        .setType(UICommodityType.VCPU.typeNumber())
                        .build();
        Mockito.when(commodityConverter.marketToTopologyCommodity(commSpec))
                        .thenReturn(Optional.of(topologyCommType));
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, e -> {
            e.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(topologyCommType)
                            .setHotResizeInfo(HotResizeInfo.newBuilder()
                                            .setHotAddSupported(true)
                                            .setHotRemoveSupported(true)));
            final VirtualMachineInfo.Builder vmBuilder =
                            e.getTypeSpecificInfoBuilder().getVirtualMachineBuilder();
            vmBuilder.setCpuScalingPolicy(CpuScalingPolicy.newBuilder().setSockets(3).build());
            vmBuilder.setCoresPerSocketChangeable(cpsChangeable);
        }, t -> { });
        Mockito.when(commodityIndex.getCommSold(entityId, topologyCommType))
                        .thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                        .setSellingTrader(entityId)
                        .setOldCapacity(1)
                        .setNewCapacity(2)
                        .setReasonCommodity(commSpec)
                        .setSpecification(commSpec)
                        .setScalingGroupId("foo")
                        .build();

        final Resize resize = resizeInterpreter.interpret(resizeTO, projectedTopology).get();
        Assert.assertThat(resize.getCommodityType(), is(topologyCommType));
        Assert.assertThat(resize.getNewCapacity(), is(cpsChangeable ? 3.0f : 2.0f));
        Assert.assertThat(resize.getOldCapacity(), is(1.0f));
        Assert.assertThat(resize.getScalingGroupId(), is("foo"));
        Assert.assertThat(resize.getHotAddSupported(), is(true));
        Assert.assertThat(resize.getHotRemoveSupported(), is(true));
        Assert.assertThat(resize.hasNewCpsr(), CoreMatchers.is(cpsChangeable));
        Assert.assertThat(resize.hasOldCpsr(), CoreMatchers.is(cpsChangeable));
    }

    /**
     * Interpret a resize with no special cases.
     */
    @Test
    public void testInterpretNormalResize() {

        final long entityId = 7;
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build();
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, e -> {
            e.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(topologyCommType)
                    .setHotResizeInfo(HotResizeInfo.newBuilder()
                            .setHotAddSupported(true)
                            .setHotRemoveSupported(true)));
        }, t -> { });

        ResizeTO resizeTO = makeResize(entityId, 1, 2, UICommodityType.VMEM, "foo");

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
     * Test the VM new capacity is rounded up to nearest multiple of cores per socket and PM cannot host it.
     */
    @Test
    public void testVMNewCapacityRoundedForPolicyCoresPerSocket() {
        Optional<Resize> resize = resizeForRoundedCapacity(8, 5, 1,
                2, true);
        //cores per socket is 2 and new capacity is 5, it needs to be rounded to 6.
        Assert.assertTrue(resize.isPresent());
        Assert.assertEquals(6, resize.get().getNewCapacity(), SMAUtils.BIG_EPSILON);
    }

    /**
     * Test the VM new capacity is rounded up to nearest multiple of cores per socket and PM cannot host it.
     */
    @Test
    public void testVMNewCapacityRoundedForCurrentCoresPerSocket() {
        Optional<Resize> resize = resizeForRoundedCapacity(8, 5, 2,
                null, true);
        //cores per socket is 2 and new capacity is 5, it needs to be rounded to 6.
        Assert.assertTrue(resize.isPresent());
        Assert.assertEquals(6, resize.get().getNewCapacity(), 0.001);
    }

    /**
     * Test the VM new capacity shouldn't be rounded if cores per socket is not changeable.
     */
    @Test
    public void testVMNewCapacityShouldNotRoundWhenCoresPerSocketNotChangeable() {
        Optional<Resize> resize = resizeForRoundedCapacity(8, 5, 1,
                2, false);
        Assert.assertTrue(resize.isPresent());
        Assert.assertEquals(5, resize.get().getNewCapacity(), 0.001);
    }

    /**
     * Test the VM new capacity shouldn't be rounded if cores per socket is not changeable.
     */
    @Test
    public void testActionDroppedWhenPmCantHostRoundedCapacity() {
        Optional<Resize> resize = resizeForRoundedCapacity(4, 5, 1,
                2, true);
        Assert.assertFalse(resize.isPresent());
    }

    private Optional<Resize> resizeForRoundedCapacity(int projectedHostCpuThreads,
            int vmNewCapacity, int vmCurrentCoresPerSocket, Integer vmPolicyCoresPerSocket, boolean coresPerSocketChangeable) {
        long pmOid = 666;
        long vmOid = 555;

        addEntity(pmOid, ApiEntityType.PHYSICAL_MACHINE, pm -> {
             pm.setTypeSpecificInfo(
                    TypeSpecificInfo.newBuilder()
                            .setPhysicalMachine(
                                    PhysicalMachineInfo.newBuilder().setNumCpuThreads(projectedHostCpuThreads)
                            ));
        }, t -> { });

        addEntity(vmOid, ApiEntityType.VIRTUAL_MACHINE, vm -> {
            VirtualMachineInfo.Builder vmInfoBuilder = VirtualMachineInfo.newBuilder()
                    .setNumCpus(4)
                    .setCoresPerSocketRatio(vmCurrentCoresPerSocket)
                    .setCoresPerSocketChangeable(coresPerSocketChangeable);
            if (vmPolicyCoresPerSocket != null) {
                vmInfoBuilder.setCpuScalingPolicy(
                        CpuScalingPolicy.newBuilder()
                                .setCoresPerSocket(vmPolicyCoresPerSocket));
            }
            vm.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(pmOid)
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                    .setTypeSpecificInfo(
                            TypeSpecificInfo.newBuilder()
                                    .setVirtualMachine(vmInfoBuilder)
                    );
        }, t -> { });

        ResizeTO resizeTO = makeResize(vmOid, 4, vmNewCapacity, UICommodityType.VCPU, "foo");

        return resizeInterpreter.interpret(resizeTO, projectedTopology);
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
        CommodityType topologyCommType = CommodityType.newBuilder()
            .setType(UICommodityType.VCPU.typeNumber())
            .build();
        final long entityId = 7;
        addEntity(entityId, ApiEntityType.VIRTUAL_MACHINE, eBldr ->
            eBldr.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus(numCpus).build())
                .build()), t -> { });
        ResizeTO resizeTO = makeResize(entityId, oldCapacity, newCapacity, UICommodityType.VCPU, "foo");

        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.of(
                CommoditySoldDTO.newBuilder().setCommodityType(CommodityType.newBuilder().setType(123))
                        .setScalingFactor(scalingFactor).build()));

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
        assertThat(resize.get().getOldCapacity(), is(0.5f));
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

    private ResizeTO makeResize(long entityId, float oldCapacity, float newCapacity,
            UICommodityType commodityType, String scalingGroup) {
        CommoditySpecificationTO commSpec = CommoditySpecificationTO.newBuilder()
                .setType(commodityType.typeNumber())
                .setBaseType(commodityType.typeNumber())
                .build();
        CommodityType topologyCommType = CommodityType.newBuilder()
                .setType(commodityType.typeNumber())
                .build();
        when(commodityConverter.marketToTopologyCommodity(commSpec)).thenReturn(Optional.of(topologyCommType));
        when(commodityIndex.getCommSold(entityId, topologyCommType)).thenReturn(Optional.empty());
        ResizeTO resizeTO = ResizeTO.newBuilder()
                .setSellingTrader(entityId)
                .setOldCapacity(oldCapacity)
                .setNewCapacity(newCapacity)
                .setReasonCommodity(commSpec)
                .setSpecification(commSpec)
                .setScalingGroupId(scalingGroup)
                .build();
        return resizeTO;
    }

    /**
     * Clear projectedTopology for each test.
     */
    @After
    public void clear() {
        projectedTopology.clear();
    }
}
