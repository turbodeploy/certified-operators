package com.vmturbo.market.runner.reservedcapacity;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityAnalysisEngine;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityResults;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ReservedCapacityAnalysisEngine}.
 */
public class ReservedCapacityAnalysisEngineTest {

    private static final double FLOATING_POINT_DELTA = 1e-7;

    private static final Long PM_OID = 1L;
    private static final Long VM_OID = 2L;

    private static final CommodityType MEM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.MEM_VALUE).build();
    private static final CommodityType VMEM = CommodityType.newBuilder()
        .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

    private TopologyEntityDTO.Builder pm;
    private TopologyEntityDTO.Builder[] vms;
    private TopologyEntityDTO.Builder vm;
    private TopologyEntityDTO.Builder[] containers;
    private TopologyEntityDTO.Builder container;

    private CommodityBoughtDTO.Builder memBought;
    private CommoditySoldDTO.Builder vmemSold;
    private ConsistentScalingHelper csh = mock(ConsistentScalingHelper.class);

    /**
     * Identity initialization to run before each test.
     */
    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Scenario data for test VM creation.
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

    /**
     * Scenario data for test Container creation.
     * { oid, current reservation, used, peak, increment, consistent-scaling }
     */
    class ContainerScenario {
        Long oid;
        double currentReservation;
        double used;
        double peak;
        float increment;
        boolean consistentScaling;

        ContainerScenario(Long oid, double currentReservation, double used,
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
        new VMScenario(4L, 50, 10, 20, 15, true),
        new VMScenario(5L, 50, 10, 20, 50, true)
    };

    ContainerScenario[] containerData = {
        new ContainerScenario(6L, 100, 10, 20, 15, false),
        new ContainerScenario(7L, 100, 30, 60, 15, true),
        new ContainerScenario(8L, 50, 10, 20, 15, true),
        new ContainerScenario(9L, 50, 10, 20, 50, true)
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

    private TopologyEntityDTO.Builder[] createContainers() {
        TopologyEntityDTO.Builder[] containers = new TopologyEntityDTO.Builder[containerData.length];
        int i = 0;
        for (ContainerScenario container : containerData) {
            TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.CONTAINER_VALUE)
                .setOid(container.oid)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true));
            if (!container.consistentScaling) {
                // Our ancient version of mockito doesn't return Optional.empty() for optional
                // return values, so I need to do it here.
                when(csh.getScalingGroupId(eq(container.oid))).thenReturn(Optional.empty());
            } else {
                when(csh.getScalingGroupId(eq(container.oid))).thenReturn(Optional.of("scaling-group-1"));
                // Pre-define comms bought/sold for scaling group VMs.  The pre-scaling group
                // unit tests manually set them up.
                builder
                    .addCommoditySoldList(makeVMemSold(container.used, container.peak, container.increment))
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(VM_OID)
                        .addCommodityBought(makeMemBought(container.currentReservation)));
            }
            containers[i++] = builder;
        }
        return containers;
    }

    /**
     * Common code before each test.
     */
    @Before
    public void setup() {
        pm = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(PM_OID)
            .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true));
        vms = createVMs();
        vm = vms[0];
        containers = createContainers();
        container = containers[0];
        memBought = makeMemBought(100);
        vmemSold = makeVMemSold(10, 20, 15);
    }

    private ReservedCapacityResults makeRCA(ConsistentScalingHelper csh, TopologyEntityDTO.Builder... builders) {
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        for (Builder builder : builders) {
            topology.put(builder.getOid(), builder.build());
        }
        return new ReservedCapacityAnalysisEngine().execute(topology, csh);
    }

    /**
     * We only generate reservation resize actions for a VM or a container.
     */
    @Test
    public void testEntityTypeNotInReservedEntityType() {
        ReservedCapacityResults rca = makeRCA(null, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * which is not controllable.
     */
    @Test
    public void testNotControllable() {
        vm.getAnalysisSettingsBuilder().setControllable(false);
        ReservedCapacityResults rca = makeRCA(null, vm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for an idle VM or an idle container.
     */
    @Test
    public void testIdleVM() {
        vm.getAnalysisSettingsBuilder().setControllable(true);
        vm.setEntityState(EntityState.POWERED_OFF);
        ReservedCapacityResults rca = makeRCA(null, vm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought doesn't have a provider.
     */
    @Test
    public void testNoProvider() {
        vm.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a provider not in the oidToDto map.
     */
    @Test
    public void testProviderIdNotInOidToDtoMap() {
        vm.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a provider which is not controllable.
     */
    @Test
    public void testProviderNotControllable() {
        vm.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(memBought));
        pm.getAnalysisSettingsBuilder().setControllable(false);
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought doesn't have reservedCapacity.
     */
    @Test
    public void testNoReservedCapacity() {
        vm.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(memBought.setReservedCapacity(0)));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought doesn't have a corresponding commSold.
     */
    @Test
    public void testNoCorrespondingCommSold() {
        vm.addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderId(PM_OID)
            .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a corresponding commSold which is not resizeable.
     */
    @Test
    public void testCorrespondingCommSoldNotResizeable() {
        vmemSold.setIsResizeable(false);
        vm.addCommoditySoldList(vmemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a corresponding commSold with large used or peak value.
     */
    @Test
    public void testLargePeakValue() {
        vm.addCommoditySoldList(vmemSold.setUsed(1000).setPeak(2000))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * We don't generate reservation resize actions for a VM or a container
     * whose commodityBought has a corresponding commSold with large capacityIncrement.
     */
    @Test
    public void testLargeCapacityIncrement() {
        vm.addCommoditySoldList(vmemSold.setCapacityIncrement(90))
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * Check if the reservation resize action is correct.
     */
    @Test
    public void testReservationResizeAction() {
        vm.addCommoditySoldList(vmemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(1, rca.getActions().size());
        assertEquals(25, rca.getReservedCapacity(VM_OID, MEM), FLOATING_POINT_DELTA);
        Action action = rca.getActions().iterator().next();
        assertEquals(-1, action.getDeprecatedImportance(), FLOATING_POINT_DELTA);
        assertTrue(action.getExecutable());
        ResizeExplanation explanation = action.getExplanation().getResize();
        assertEquals(25, explanation.getDeprecatedStartUtilization(), FLOATING_POINT_DELTA);
        assertEquals(100, explanation.getDeprecatedEndUtilization(), FLOATING_POINT_DELTA);
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
        ReservedCapacityResults rca = makeRCA(csh, vms[1], vms[2], vms[3], pm);

        assertEquals(1, rca.getActions().size());
        assertEquals(60, rca.getReservedCapacity(vms[1].getOid(), MEM), FLOATING_POINT_DELTA);
        Action action = rca.getActions().iterator().next();
        assertTrue(action.getExecutable());
        assertTrue(action.hasExplanation());
        assertTrue(action.getExplanation().hasResize());
        ResizeExplanation explanation = action.getExplanation().getResize();
        assertEquals(60, explanation.getDeprecatedStartUtilization(), FLOATING_POINT_DELTA);
        assertEquals(100, explanation.getDeprecatedEndUtilization(), FLOATING_POINT_DELTA);
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
     * Verify consistent reservation actions with idle VM.
     */
    @Test
    public void testConsistentReservationActionsWithIdleVM() {
        /*
         * VM-1 has current reservation = 100, and is powered off
         * VM-2 has current reservation = 50, new reservation = 20
         *
         * The maximum new reservation for the scaling group is therefore 20.
         * We should see a single reservation from 50 -> 20 for VM-2.
         */
        vms[1].setEntityState(EntityState.POWERED_OFF);
        ReservedCapacityResults rca = makeRCA(csh, vms[1], vms[2], pm);

        assertEquals(1, rca.getActions().size());
        assertEquals(20, rca.getReservedCapacity(vms[2].getOid(), MEM), FLOATING_POINT_DELTA);
        Action action = rca.getActions().iterator().next();
        assertTrue(action.getExecutable());
        assertTrue(action.hasExplanation());
        assertTrue(action.getExplanation().hasResize());
        ResizeExplanation explanation = action.getExplanation().getResize();
        assertEquals(20, explanation.getDeprecatedStartUtilization(), FLOATING_POINT_DELTA);
        assertEquals(50, explanation.getDeprecatedEndUtilization(), FLOATING_POINT_DELTA);
        Resize resize = action.getInfo().getResize();
        assertEquals(20, resize.getNewCapacity(), FLOATING_POINT_DELTA);
        assertEquals(50, resize.getOldCapacity(), FLOATING_POINT_DELTA);
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
    public void testReservationEqualToCapacityVM() {
        vm.addCommoditySoldList(vmemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(memBought.setReservedCapacity(200)));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }

    /**
     * Reservation capacity == container.
     */
    @Test
    public void testReservationEqualToCapacityContainer() {
        container.addCommoditySoldList(vmemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(VM_OID)
                .addCommodityBought(memBought.setReservedCapacity(200)));
        ReservedCapacityResults rca = makeRCA(null, container, vm);
        assertEquals(1, rca.getActions().size());
    }

    /**
     * Container-VM reservation.
     */
    @Test
    public void testReservationNonResizableContainer() {
        vmemSold.setIsResizeable(false);
        container.addCommoditySoldList(vmemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(VM_OID)
                .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, container, vm);

        assertEquals(1, rca.getActions().size());
    }

    /**
     * VM-PM reservation.
     */
    @Test
    public void testReservationNonResizableVM() {
        vmemSold.setIsResizeable(false);
        vm.addCommoditySoldList(vmemSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(memBought));
        ReservedCapacityResults rca = makeRCA(null, vm, pm);

        assertEquals(0, rca.getActions().size());
    }
}
