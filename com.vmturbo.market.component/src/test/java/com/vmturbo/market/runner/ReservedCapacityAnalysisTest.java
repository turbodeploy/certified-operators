package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ReservedCapacityAnalysisTest {

    private static final double FLOATING_POINT_DELTA = 1e-7;

    private static final Long PM_OID = 1L;
    private static final Long VM_OID = 2L;

    private static final CommodityType MEM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.MEM_VALUE).build();
    private static final CommodityType VMEM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

    private static TopologyEntityDTO.Builder PM;
    private static TopologyEntityDTO.Builder VM;
    private static CommodityBoughtDTO.Builder MemBought;
    private static CommoditySoldDTO.Builder VMemSold;

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    @Before
    public void setup() {
        PM = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(PM_OID)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true));
        VM = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(VM_OID)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true));
        MemBought = CommodityBoughtDTO.newBuilder()
            .setCommodityType(MEM)
            .setReservedCapacity(100);
        VMemSold = CommoditySoldDTO.newBuilder()
            .setCommodityType(VMEM)
            .setIsResizeable(true)
            .setUsed(10)
            .setPeak(20)
            .setCapacityIncrement(15);
    }

    /**
     * We only generate reservation resize actions for a VM or a container.
     */
    @Test
    public void testEntityTypeNotInReservedEntityType() {
        ReservedCapacityAnalysis rca = new ReservedCapacityAnalysis(ImmutableMap.of(PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * which is not controllable.
     */
    @Test
    public void testNotControllable() {
        VM.getAnalysisSettingsBuilder().setControllable(false);
        ReservedCapacityAnalysis rca = new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought doesn't have a provider.
     */
    @Test
    public void testNoProvider() {
        VM.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca = new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a provider not in the oidToDto map.
     */
    @Test
    public void testProviderIdNotInOidToDtoMap() {
        VM.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca = new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a provider which is not controllable.
     */
    @Test
    public void testProviderNotControllable() {
       VM.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
           .setProviderId(PM_OID)
           .addCommodityBought(MemBought));
        PM.getAnalysisSettingsBuilder().setControllable(false);
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought doesn't have reservedCapacity.
     */
    @Test
    public void testNoReservedCapacity() {
        VM.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(MemBought.setReservedCapacity(0)));
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought doesn't have a corresponding commSold.
     */
    @Test
    public void testNoCorrespondingCommSold() {
        VM.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a corresponding commSold which is not resizeable.
     */
    @Test
    public void testCorrespondingCommSoldNotResizeable() {
        VMemSold.setIsResizeable(false);
        VM.addCommoditySoldList(VMemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a corresponding commSold with large used or peak value.
     */
    @Test
    public void testLargePeakValue() {
        VM.addCommoditySoldList(VMemSold.setUsed(1000).setPeak(2000))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a corresponding commSold with large capacityIncrement.
     */
    @Test
    public void testLargeCapacityIncrement() {
        VM.addCommoditySoldList(VMemSold.setCapacityIncrement(90))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(0, rca.getActions().size());
    }

    /**
     * Check if the reservation resize action is correct.
     */
    @Test
    public void testReservationResizeAction() {
        VM.addCommoditySoldList(VMemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(MemBought));
        ReservedCapacityAnalysis rca =
            new ReservedCapacityAnalysis(ImmutableMap.of(VM_OID, VM.build(), PM_OID, PM.build()));
        rca.execute();

        assertEquals(1, rca.getActions().size());
        assertEquals(25, rca.getReservedCapacity(VM_OID, MEM), FLOATING_POINT_DELTA);
        Action action = rca.getActions().iterator().next();
        assertEquals(-1, action.getImportance(), FLOATING_POINT_DELTA);
        assertTrue(action.getExecutable());
        ResizeExplanation explanation = action.getExplanation().getResize();
        assertEquals(25, explanation.getStartUtilization(), FLOATING_POINT_DELTA);
        assertEquals(100, explanation.getEndUtilization(), FLOATING_POINT_DELTA);
        Resize resize = action.getInfo().getResize();
        assertEquals(25, resize.getNewCapacity(), FLOATING_POINT_DELTA);
        assertEquals(100, resize.getOldCapacity(), FLOATING_POINT_DELTA);
        assertEquals(MEM, resize.getCommodityType());
        assertEquals(CommodityAttribute.RESERVED, resize.getCommodityAttribute());
    }
}
