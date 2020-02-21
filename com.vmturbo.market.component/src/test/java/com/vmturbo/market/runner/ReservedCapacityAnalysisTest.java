package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
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
    private static TopologyEntityDTO.Builder[] VMs;
    private static TopologyEntityDTO.Builder VM;
    private static CommodityBoughtDTO.Builder MemBought;
    private static CommoditySoldDTO.Builder VMemSold;
    private static ConsistentScalingHelper csh = mock(ConsistentScalingHelper.class);

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    /*
     * Scenario data for test VM creation:
     * { oid, current reservation, used, peak, increment, consistent-scaling }
     */
    class VMScenario {
        Long oid;
        double currentReservation;
        double used;
        double peak;
        float increment;
        boolean consistentScaling;

        VMScenario(Long oid, double currentReservation, double used,
                   double peak, float increment, boolean consistentScaling) {
            this.oid = oid;
            this.currentReservation = currentReservation;
            this.used = used;
            this.peak = peak;
            this.increment = increment;
            this.consistentScaling = consistentScaling;
        }
    }

    VMScenario[] vmData = {
        new VMScenario(2L, 100, 10, 20, 15, false),
        new VMScenario(3L, 100, 30, 60, 15, true),
        new VMScenario(4L, 50,  10, 20, 15, true),
        new VMScenario(5L, 50,  10, 20, 50, true)
    };

    private CommodityBoughtDTO.Builder makeMemBought(double capacity) {
        return CommodityBoughtDTO.newBuilder()
            .setCommodityType(MEM)
            .setReservedCapacity(capacity);
    }

    private CommoditySoldDTO.Builder makeVMemSold(double used, double peak, float increment) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(VMEM)
            .setIsResizeable(true)
            .setUsed(used)
            .setPeak(peak)
            .setCapacityIncrement(increment)
            .setCapacity(200);
    }

    private TopologyEntityDTO.Builder[] createVMs() {
        TopologyEntityDTO.Builder[] vms = new TopologyEntityDTO.Builder[vmData.length];
        int i = 0;
        for (VMScenario vm : vmData) {
            TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(vm.oid)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true));
            if (!vm.consistentScaling) {
                // Our ancient version of mockito doesn't return Optional.empty() for optional
                // return values, so I need to do it here.
                when(csh.getScalingGroupId(eq(vm.oid))).thenReturn(Optional.empty());
            } else {
                when(csh.getScalingGroupId(eq(vm.oid))).thenReturn(Optional.of("scaling-group-1"));
                // Pre-define comms bought/sold for scaling group VMs.  The pre-scaling group
                // unit tests manually set them up.
                builder
                    .addCommoditySoldList(makeVMemSold(vm.used, vm.peak, vm.increment))
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PM_OID)
                        .addCommodityBought(makeMemBought(vm.currentReservation)));
            }
            vms[i++] = builder;
        }
        return vms;
    }

    @Before
    public void setup() {
        PM = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(PM_OID)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true));
        VMs = createVMs();
        VM = VMs[0];
        MemBought = makeMemBought(100);
        VMemSold = makeVMemSold(10, 20, 15);
    }

    private ReservedCapacityAnalysis makeRCA(TopologyEntityDTO.Builder... builders) {
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        for (Builder builder : builders) {
            topology.put(builder.getOid(), builder.build());
        }
        return new ReservedCapacityAnalysis(topology);
    }

    /**
     * We only generate reservation resize actions for a VM or a container.
     */
    @Test
    public void testEntityTypeNotInReservedEntityType() {
        ReservedCapacityAnalysis rca = makeRCA(PM);
        rca.execute(null);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * which is not controllable.
     */
    @Test
    public void testNotControllable() {
        VM.getAnalysisSettingsBuilder().setControllable(false);
        ReservedCapacityAnalysis rca = makeRCA(VM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

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
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

        assertEquals(1, rca.getActions().size());
        assertEquals(25, rca.getReservedCapacity(VM_OID, MEM), FLOATING_POINT_DELTA);
        Action action = rca.getActions().iterator().next();
        assertEquals(-1, action.getDeprecatedImportance(), FLOATING_POINT_DELTA);
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

    /**
     * Verify consistent reservation actions.
     */
    @Test
    public void testConsistentReservationActions() {
        /*
         * VM-1 has current reservation = 100, new reservation = 60
         * VM-2 has current reservation = 50, new reservation = 20
         * VM-3 has current reservation = 100, new reservation = 60, increment = 50
         *
         * The maximum new reservation for the scaling group is therefore 60.
         * Since 60 is greater than the current reservation in VM-2, no
         * reservation action will be generated for it.  We should see a single
         * reservation from 100 -> 60 for VM-1.  VM-3 also wants to go from 100 -> 60, but the
         * delta of 40 is less than its used increment is 50, so the action will not be generated.
         */
        ReservedCapacityAnalysis rca = makeRCA(VMs[1], VMs[2], VMs[3], PM);
        rca.execute(csh);

        assertEquals(1, rca.getActions().size());
        assertEquals(60, rca.getReservedCapacity(VMs[1].getOid(), MEM), FLOATING_POINT_DELTA);
        Action action = rca.getActions().iterator().next();
        assertTrue(action.getExecutable());
        assertTrue(action.hasExplanation());
        assertTrue(action.getExplanation().hasResize());
        ResizeExplanation explanation = action.getExplanation().getResize();
        assertEquals(60, explanation.getStartUtilization(), FLOATING_POINT_DELTA);
        assertEquals(100, explanation.getEndUtilization(), FLOATING_POINT_DELTA);
        Resize resize = action.getInfo().getResize();
        assertEquals(60, resize.getNewCapacity(), FLOATING_POINT_DELTA);
        assertEquals(100, resize.getOldCapacity(), FLOATING_POINT_DELTA);
        assertEquals(MEM, resize.getCommodityType());
        assertEquals(CommodityAttribute.RESERVED, resize.getCommodityAttribute());
        assertTrue(resize.hasScalingGroupId());
        assertTrue(explanation.hasScalingGroupId());
        assertEquals("scaling-group-1", explanation.getScalingGroupId());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a reservation value at or higher than the capacity of the
     * commodity sold. This is because the reservation is either locked to capacity or there
     * is some misconfiguration and attempting to execute the action will fail if the resize
     * is still about capacity.
     */
    @Test
    public void testReservationEqualToCapacity() {
        VM.addCommoditySoldList(VMemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(MemBought.setReservedCapacity(200)));
        ReservedCapacityAnalysis rca = makeRCA(VM, PM);
        rca.execute(null);

        assertEquals(0, rca.getActions().size());
    }
}
