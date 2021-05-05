package com.vmturbo.topology.processor.controllable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class ControllableManagerTest {

    private final EntityActionDao entityActionDao = Mockito.mock(EntityActionDao.class);

    private final EntityMaintenanceTimeDao entityMaintenanceTimeDao = Mockito.mock(EntityMaintenanceTimeDao.class);

    private ControllableManager controllableManager;

    private final TopologyEntityDTO.Builder vmFooEntityBuilder = TopologyEntityDTO.newBuilder()
        .setOid(1)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
            .addCommoditiesBoughtFromProviders(
                TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(4));
    private final TopologyEntityDTO.Builder vmBarEntityBuilder = TopologyEntityDTO.newBuilder()
        .setOid(2)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
            .addCommoditiesBoughtFromProviders(
                TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(4));
    private final TopologyEntityDTO.Builder vmBazEntityBuilder = TopologyEntityDTO.newBuilder()
        .setOid(3)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(5))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CommodityType.newBuilder().setType(53)));
    private final TopologyEntityDTO.Builder pmInMaintenance = TopologyEntityDTO.newBuilder()
        .setOid(4)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE);
    private final TopologyEntityDTO.Builder pmPoweredOn = TopologyEntityDTO.newBuilder()
        .setOid(5)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.POWERED_ON);

    private final TopologyEntityDTO.Builder container1 = TopologyEntityDTO.newBuilder()
        .setOid(10)
        .setEntityType(EntityType.CONTAINER_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(11));
    private final TopologyEntityDTO.Builder pod1 = TopologyEntityDTO.newBuilder()
        .setOid(11)
        .setEntityType(EntityType.CONTAINER_POD_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(13));
    private final TopologyEntityDTO.Builder pod2 = TopologyEntityDTO.newBuilder()
        .setOid(12)
        .setEntityType(EntityType.CONTAINER_POD_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(14));
    private final TopologyEntityDTO.Builder vm1 = TopologyEntityDTO.newBuilder()
        .setOid(13)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(15));
    private final TopologyEntityDTO.Builder vm2 = TopologyEntityDTO.newBuilder()
        .setOid(14)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(15));
    private final TopologyEntityDTO.Builder VDC = TopologyEntityDTO.newBuilder()
        .setOid(16)
        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(15));
    private final TopologyEntityDTO.Builder pmFailover = TopologyEntityDTO.newBuilder()
        .setOid(15)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.FAILOVER);

    private final TopologyEntityDTO.Builder pmInMaintenance1 = TopologyEntityDTO.newBuilder()
        .setOid(21)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE);
    private final TopologyEntityDTO.Builder vmMaintenance1 = TopologyEntityDTO.newBuilder()
        .setOid(22)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(pmInMaintenance1.getOid()))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CommodityType.newBuilder().setType(53)));
    private final TopologyEntityDTO.Builder vmMaintenance2 = TopologyEntityDTO.newBuilder()
        .setOid(23)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
        .addCommoditiesBoughtFromProviders(
            TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(pmInMaintenance1.getOid()))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(CommodityType.newBuilder().setType(53)));

    private final static TopologyEntityDTO.Builder pmInMaintenance2 = TopologyEntityDTO.newBuilder()
        .setOid(100)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
            PhysicalMachineInfo.newBuilder().setAutomationLevel(AutomationLevel.FULLY_AUTOMATED)));
    private final static TopologyEntityDTO.Builder pmInMaintenance3 = TopologyEntityDTO.newBuilder()
        .setOid(101)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
            PhysicalMachineInfo.newBuilder().setAutomationLevel(AutomationLevel.PARTIALLY_AUTOMATED)));
    private final static TopologyEntityDTO.Builder pmInMaintenance4 = TopologyEntityDTO.newBuilder()
        .setOid(102)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(AnalysisSettings.newBuilder()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(
            PhysicalMachineInfo.newBuilder().setAutomationLevel(AutomationLevel.NOT_AUTOMATED)));

    private final Map<Long, Builder> topology = new HashMap<>();

    private TopologyGraph<TopologyEntity> topologyGraph;

    @Before
    public void setup() {
        controllableManager = new ControllableManager(entityActionDao, entityMaintenanceTimeDao, true);
        topology.put(vmFooEntityBuilder.getOid(), TopologyEntity.newBuilder(vmFooEntityBuilder));
        topology.put(vmBarEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBarEntityBuilder));
        topology.put(vmBazEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBazEntityBuilder));
        topology.put(pmInMaintenance.getOid(), TopologyEntity.newBuilder(pmInMaintenance)
            .addConsumer(topology.get(vmFooEntityBuilder.getOid()))
            .addConsumer(topology.get(vmBarEntityBuilder.getOid())));
        topology.put(pmPoweredOn.getOid(), TopologyEntity.newBuilder(pmPoweredOn)
            .addConsumer(topology.get(vmBazEntityBuilder.getOid())));

        topology.put(vmMaintenance1.getOid(), TopologyEntity.newBuilder(vmMaintenance1));
        topology.put(vmMaintenance2.getOid(), TopologyEntity.newBuilder(vmMaintenance2));
        topology.put(pmInMaintenance1.getOid(), TopologyEntity.newBuilder(pmInMaintenance1)
            .addConsumer(topology.get(vmMaintenance1.getOid()))
            .addConsumer(topology.get(vmMaintenance2.getOid())));

        topology.put(container1.getOid(), TopologyEntity.newBuilder(container1));
        topology.put(pod1.getOid(), TopologyEntity.newBuilder(pod1)
            .addConsumer(topology.get(container1.getOid())));
        topology.put(pod2.getOid(), TopologyEntity.newBuilder(pod2));
        topology.put(vm1.getOid(), TopologyEntity.newBuilder(vm1)
            .addConsumer(topology.get(pod1.getOid())));
        topology.put(vm2.getOid(), TopologyEntity.newBuilder(vm2)
            .addConsumer(topology.get(pod2.getOid())));
        topology.put(VDC.getOid(), TopologyEntity.newBuilder(VDC));
        topology.put(pmFailover.getOid(), TopologyEntity.newBuilder(pmFailover)
            .addConsumer(topology.get(vm1.getOid()))
            .addConsumer(topology.get(vm2.getOid()))
            .addConsumer(topology.get(VDC.getOid())));

        topology.put(pmInMaintenance2.getOid(), TopologyEntity.newBuilder(pmInMaintenance2));
        topology.put(pmInMaintenance3.getOid(), TopologyEntity.newBuilder(pmInMaintenance3));
        topology.put(pmInMaintenance4.getOid(), TopologyEntity.newBuilder(pmInMaintenance4));

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topology);
    }

    /**
     * Test that the correct set of entities have their controllable bit reset given the list of ids
     * returned by the database is correct.
     *
     * <p>Currently it doesn't check for side-effects.</p>
     */
    @Test
    public void testApplyControllableEntityAction() {
        when(entityActionDao.getNonControllableEntityIds())
                .thenReturn(Sets.newHashSet(1L, 3L, 4L));
        controllableManager.applyControllable(topologyGraph);
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getControllable());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getControllable());
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance.getAnalysisSettings().getControllable());
        assertTrue(pmPoweredOn.getAnalysisSettings().getControllable());
    }

    /**
     * Make sure that VMs on a failover host are not controllable.
     * Topology contains a failover host, two VMs and one VDC on it, two container pods and one container.
     * Only two VMs should be controllable false.
     */
    @Test
    public void testApplyControllableFailoverHost() {
        when(entityActionDao.getNonControllableEntityIds())
            .thenReturn(Collections.emptySet());
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(3, numModified);
        assertTrue(container1.getAnalysisSettings().getControllable());
        assertTrue(pod1.getAnalysisSettings().getControllable());
        assertTrue(pod2.getAnalysisSettings().getControllable());
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
        assertTrue(VDC.getAnalysisSettings().getControllable());
        assertTrue(pmFailover.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
    }

    /**
     * Make sure that VMs on a unknown host are not controllable.
     */
    @Test
    public void testApplyControllableUnknownHost() {
        topology.get(pmFailover.getOid()).getEntityBuilder().setEntityState(EntityState.UNKNOWN);
        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topology);

        Mockito.when(entityActionDao.getNonControllableEntityIds())
            .thenReturn(Collections.emptySet());
        assertEquals(EntityState.UNKNOWN, pmFailover.getEntityState());
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(3, numModified);
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
    }

    /**
     * Suspended entities should be marked as not controllable.
     */
    @Test
    public void testApplyControllableSuspendedEntity() {
        final TopologyEntityDTO.Builder suspendedVM = TopologyEntityDTO.newBuilder()
            .setOid(201)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setControllable(true))
            .setEntityState(EntityState.SUSPENDED)
            .addCommoditiesBoughtFromProviders(
                TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(pmPoweredOn.getOid()));
        final TopologyEntityDTO.Builder suspendedContainer = TopologyEntityDTO.newBuilder()
            .setOid(202)
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                .setControllable(true))
            .setEntityState(EntityState.SUSPENDED)
            .addCommoditiesBoughtFromProviders(
                TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(pod2.getOid()));

        topology.put(suspendedVM.getOid(), TopologyEntity.newBuilder(suspendedVM));
        topology.put(suspendedContainer.getOid(), TopologyEntity.newBuilder(suspendedContainer));
        topology.get(pmPoweredOn.getOid()).addConsumer(topology.get(suspendedVM.getOid()));
        topology.get(pod2.getOid()).addConsumer(topology.get(suspendedContainer.getOid()));

        topologyGraph = TopologyEntityTopologyGraphCreator.newGraph(topology);
        Mockito.when(entityActionDao.getNonControllableEntityIds())
            .thenReturn(Collections.emptySet());
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(5, numModified);
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
        assertFalse(suspendedContainer.getAnalysisSettings().getControllable());
        assertFalse(suspendedContainer.getAnalysisSettings().getControllable());
    }

    /**
     * Make sure all hosts in Maintenance mode and with automation level equal to FULLY_AUTOMATED are set to non-controllable.
     */
    @Test
    public void testApplyControllableAutomationLevel() {
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(3, numModified);
        assertTrue(pmInMaintenance.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance1.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance3.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance4.getAnalysisSettings().getControllable());
        assertTrue(pmFailover.getAnalysisSettings().getControllable());
    }

    /**
     * When maintenance mode feature is disabled, controllable flag doesn't change.
     */
    @Test
    public void testApplyControllableAutomationLevelFeatureDisabled() {
        controllableManager = new ControllableManager(entityActionDao, entityMaintenanceTimeDao, false);
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(2, numModified);
        assertTrue(pmInMaintenance2.getAnalysisSettings().getControllable());
    }

    /**
     * Test application of scale.
     */
    @Test
    public void testApplyScale() {
        when(entityActionDao.ineligibleForScaleEntityIds())
                .thenReturn(Sets.newHashSet(1L, 2L, 3L));
        controllableManager.applyScaleEligibility(topologyGraph);
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
        // This is false because on this entity has VM entity type.
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
    }

    /**
     * Test application of resized down.
     */
    @Test
    public void testApplyResizeDownEligibility() {
        when(entityActionDao.ineligibleForResizeDownEntityIds())
                .thenReturn(Sets.newHashSet(1L, 2L, 3L));
        controllableManager.applyResizable(topologyGraph);
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
        // This is true because on this entity has VM entity type.
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
    }

    /**
     * Test VMs on maintenance host.
     */
    @Test
    public void testMarkVMsOnMaintenanceHostAsNotResizable() {
        controllableManager.applyResizable(topologyGraph);
        for (CommoditySoldDTO.Builder commSold : vmMaintenance1.getCommoditySoldListBuilderList()) {
            assertFalse(commSold.getIsResizeable());
        }
        for (CommoditySoldDTO.Builder commSold : vmMaintenance2.getCommoditySoldListBuilderList()) {
            assertFalse(commSold.getIsResizeable());
        }
        // vmBazEntityBuilder is not on maintenance host
        for (CommoditySoldDTO.Builder commSold : vmBazEntityBuilder.getCommoditySoldListBuilderList()) {
            assertTrue(commSold.getIsResizeable());
        }
    }

    /**
     * Test hosts exit maintenance mode.
     */
    @Test
    public void testKeepControllableFalseAfterExitingMaintenanceMode() {
        when(entityMaintenanceTimeDao.getControllableFalseHost())
            .thenReturn(ImmutableSet.of(pmPoweredOn.getOid(), pmInMaintenance.getOid(), pmFailover.getOid()));
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(6, numModified);
        assertFalse(pmPoweredOn.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance.getAnalysisSettings().getControllable());
        assertFalse(pmFailover.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
    }
}
