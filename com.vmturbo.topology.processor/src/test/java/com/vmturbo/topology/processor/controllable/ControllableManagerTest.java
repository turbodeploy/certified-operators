package com.vmturbo.topology.processor.controllable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.EditImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.RemovedImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.PhysicalMachineInfoImpl;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class ControllableManagerTest {
    private static final long NOW_MS = 60 * 60 * 1000L;

    private final EntityActionDao entityActionDao = Mockito.mock(EntityActionDao.class);

    private final EntityMaintenanceTimeDao entityMaintenanceTimeDao = Mockito.mock(EntityMaintenanceTimeDao.class);

    private final Clock clock = mock(Clock.class);

    private ControllableManager controllableManager;

    private final TopologyEntityImpl vmFooEntityBuilder = new TopologyEntityImpl()
        .setOid(1)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
            .addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().setProviderId(4));
    private final TopologyEntityImpl vmBarEntityBuilder = new TopologyEntityImpl()
        .setOid(2)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
            .addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().setProviderId(4));
    private final TopologyEntityImpl vmBazEntityBuilder = new TopologyEntityImpl()
        .setOid(3)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(5))
        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(new CommodityTypeImpl().setType(53)));
    private final TopologyEntityImpl pmInMaintenance = new TopologyEntityImpl()
        .setOid(4)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE);
    private final TopologyEntityImpl storageInMaintenance = new TopologyEntityImpl()
            .setOid(777)
            .setEntityType(EntityType.STORAGE_VALUE)
            .setAnalysisSettings(new AnalysisSettingsImpl()
                    .setControllable(true))
            .setEntityState(EntityState.MAINTENANCE);
    private final TopologyEntityImpl pmPoweredOn = new TopologyEntityImpl()
        .setOid(5)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.POWERED_ON);

    private final TopologyEntityImpl container1 = new TopologyEntityImpl()
        .setOid(10)
        .setEntityType(EntityType.CONTAINER_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(11));
    private final TopologyEntityImpl pod1 = new TopologyEntityImpl()
        .setOid(11)
        .setEntityType(EntityType.CONTAINER_POD_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(13));
    private final TopologyEntityImpl pod2 = new TopologyEntityImpl()
        .setOid(12)
        .setEntityType(EntityType.CONTAINER_POD_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(14));

    private final TopologyEntityImpl vm1 = new TopologyEntityImpl()
        .setOid(13)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(15));
    private final TopologyEntityImpl vm2 = new TopologyEntityImpl()
        .setOid(14)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(15));

    private final TopologyEntityImpl VDC = new TopologyEntityImpl()
        .setOid(16)
        .setEntityType(EntityType.VIRTUAL_DATACENTER_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(15));
    private final TopologyEntityImpl pmFailover = new TopologyEntityImpl()
        .setOid(15)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.FAILOVER);

    private final TopologyEntityImpl pmInMaintenance1 = new TopologyEntityImpl()
        .setOid(21)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE);
    private final TopologyEntityImpl vmMaintenance1 = new TopologyEntityImpl()
        .setOid(22)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(pmInMaintenance1.getOid()))
        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(new CommodityTypeImpl().setType(53)));
    private final TopologyEntityImpl vmMaintenance2 = new TopologyEntityImpl()
        .setOid(23)
        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true)
            .setIsEligibleForScale(true)
            .setIsEligibleForResizeDown(true))
        .addCommoditiesBoughtFromProviders(
            new CommoditiesBoughtFromProviderImpl().setProviderId(pmInMaintenance1.getOid()))
        .addCommoditySoldList(new CommoditySoldImpl().setCommodityType(new CommodityTypeImpl().setType(53)));
    private final TopologyEntityImpl dbsEntityBuilder = new TopologyEntityImpl()
            .setOid(24)
            .setEntityType(EntityType.DATABASE_SERVER_VALUE)
            .setAnalysisSettings(new AnalysisSettingsImpl()
                    .setControllable(true)
                    .setIsEligibleForScale(true)
                    .setIsEligibleForResizeDown(true));
    private final static TopologyEntityImpl pmInMaintenance2 = new TopologyEntityImpl()
        .setOid(100)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE)
        .setTypeSpecificInfo(new TypeSpecificInfoImpl().setPhysicalMachine(
            new PhysicalMachineInfoImpl().setAutomationLevel(AutomationLevel.FULLY_AUTOMATED)));
    private final static TopologyEntityImpl pmInMaintenance3 = new TopologyEntityImpl()
        .setOid(101)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE)
        .setTypeSpecificInfo(new TypeSpecificInfoImpl().setPhysicalMachine(
            new PhysicalMachineInfoImpl().setAutomationLevel(AutomationLevel.PARTIALLY_AUTOMATED)));
    private final static TopologyEntityImpl pmInMaintenance4 = new TopologyEntityImpl()
        .setOid(102)
        .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
        .setAnalysisSettings(new AnalysisSettingsImpl()
            .setControllable(true))
        .setEntityState(EntityState.MAINTENANCE)
        .setTypeSpecificInfo(new TypeSpecificInfoImpl().setPhysicalMachine(
            new PhysicalMachineInfoImpl().setAutomationLevel(AutomationLevel.NOT_AUTOMATED)));

    private final Map<Long, Builder> topology = new HashMap<>();
    private GraphWithSettings topologyGraph;

    @Before
    public void setup() {
        controllableManager = new ControllableManager(entityActionDao, entityMaintenanceTimeDao,
                        clock);
        Mockito.when(clock.millis()).thenReturn(NOW_MS);
        topology.put(vmFooEntityBuilder.getOid(), TopologyEntity.newBuilder(vmFooEntityBuilder));
        topology.put(vmBarEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBarEntityBuilder));
        topology.put(vmBazEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBazEntityBuilder));
        topology.put(dbsEntityBuilder.getOid(), TopologyEntity.newBuilder(dbsEntityBuilder));
        topology.put(pmInMaintenance.getOid(), TopologyEntity.newBuilder(pmInMaintenance)
            .addConsumer(topology.get(vmFooEntityBuilder.getOid()))
            .addConsumer(topology.get(vmBarEntityBuilder.getOid())));
        topology.put(pmPoweredOn.getOid(), TopologyEntity.newBuilder(pmPoweredOn)
            .addConsumer(topology.get(vmBazEntityBuilder.getOid())));

        topology.put(storageInMaintenance.getOid(), TopologyEntity.newBuilder(storageInMaintenance));
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

        topologyGraph = createNoSettingsGraph(topology);
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
     * Test that the uncontrollable pod entites which are in power state powered on are
     * correctly set to controllable. Also ensure that the pods which are controllable
     * already, remain as is.
     */
    @Test
    public void testSetUncontrollablePodToControllable() {
        // Pods to test unplaced uncontrollable pods logic
        final TopologyEntityImpl pod3 = new TopologyEntityImpl()
                .setOid(1111)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setAnalysisSettings(new AnalysisSettingsImpl()
                        .setControllable(false))
                .addCommoditiesBoughtFromProviders(
                        // actual VM provider in topology
                        new CommoditiesBoughtFromProviderImpl().setProviderId(13));
        final TopologyEntityImpl pod4 = new TopologyEntityImpl()
                .setOid(1112)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setAnalysisSettings(new AnalysisSettingsImpl()
                        .setControllable(false))
                .addCommoditiesBoughtFromProviders(
                        // removed VM provider in topology
                        new CommoditiesBoughtFromProviderImpl().setProviderId(1221)
                                .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE));
        final TopologyEntityImpl clonePod = new TopologyEntityImpl()
                .setOid(1113)
                .setEntityType(EntityType.CONTAINER_POD_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setAnalysisSettings(new AnalysisSettingsImpl()
                        .setControllable(false))
                .addCommoditiesBoughtFromProviders(
                        // VM provider for cloned pod
                        new CommoditiesBoughtFromProviderImpl().setProviderId(-1)
                                .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE));
        final TopologyEntityImpl vmToRemove = new TopologyEntityImpl()
                .setOid(1221)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEdit(new EditImpl()
                        // Its ok to use a random plan id, as its needed only to test an internal
                        // function, without any plan id validation.
                        .setRemoved(new RemovedImpl().setPlanId(1551)))
                .setAnalysisSettings(new AnalysisSettingsImpl()
                        .setControllable(true))
                .addCommoditiesBoughtFromProviders(
                        new CommoditiesBoughtFromProviderImpl().setProviderId(15));

        topology.put(pod3.getOid(), TopologyEntity.newBuilder(pod3));
        topology.put(pod4.getOid(), TopologyEntity.newBuilder(pod4));
        topology.put(clonePod.getOid(), TopologyEntity.newBuilder(clonePod));
        topology.put(vmToRemove.getOid(), TopologyEntity.newBuilder(vmToRemove)
                .addConsumer(topology.get(pod3.getOid())));

        topologyGraph = createNoSettingsGraph(topology);

        controllableManager.setUncontrollablePodsToControllableInPlan(topologyGraph.getTopologyGraph());
        assertTrue(pod1.getAnalysisSettings().getControllable());
        assertTrue(pod2.getAnalysisSettings().getControllable());
        // Pod with provider still in topology keeps controllable false
        assertFalse(pod3.getAnalysisSettings().getControllable());
        // Pod with provider no more in topology changes controllable to true
        assertTrue(pod4.getAnalysisSettings().getControllable());
        // Clone pod changes controllable to true
        assertTrue(clonePod.getAnalysisSettings().getControllable());
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
        assertEquals(4, numModified);
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
        topology.get(pmFailover.getOid()).getTopologyEntityImpl().setEntityState(EntityState.UNKNOWN);
        topologyGraph = createNoSettingsGraph(topology);

        Mockito.when(entityActionDao.getNonControllableEntityIds())
            .thenReturn(Collections.emptySet());
        assertEquals(EntityState.UNKNOWN, pmFailover.getEntityState());
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(4, numModified);
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
    }

    /**
     * Suspended entities should be marked as not controllable.
     */
    @Test
    public void testApplyControllableSuspendedEntity() {
        final TopologyEntityImpl suspendedVM = new TopologyEntityImpl()
            .setOid(201)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setAnalysisSettings(new AnalysisSettingsImpl()
                .setControllable(true))
            .setEntityState(EntityState.SUSPENDED)
            .addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().setProviderId(pmPoweredOn.getOid()));
        final TopologyEntityImpl suspendedContainer = new TopologyEntityImpl()
            .setOid(202)
            .setEntityType(EntityType.CONTAINER_VALUE)
            .setAnalysisSettings(new AnalysisSettingsImpl()
                .setControllable(true))
            .setEntityState(EntityState.SUSPENDED)
            .addCommoditiesBoughtFromProviders(
                new CommoditiesBoughtFromProviderImpl().setProviderId(pod2.getOid()));

        topology.put(suspendedVM.getOid(), TopologyEntity.newBuilder(suspendedVM));
        topology.put(suspendedContainer.getOid(), TopologyEntity.newBuilder(suspendedContainer));
        topology.get(pmPoweredOn.getOid()).addConsumer(topology.get(suspendedVM.getOid()));
        topology.get(pod2.getOid()).addConsumer(topology.get(suspendedContainer.getOid()));

        topologyGraph = createNoSettingsGraph(topology);
        Mockito.when(entityActionDao.getNonControllableEntityIds())
            .thenReturn(Collections.emptySet());
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(6, numModified);
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
        assertEquals(4, numModified);
        assertTrue(pmInMaintenance.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance1.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance3.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance4.getAnalysisSettings().getControllable());
        assertTrue(pmFailover.getAnalysisSettings().getControllable());
    }

    /**
     * Test application of scale.
     */
    @Test
    public void testApplyScale() {
        when(entityActionDao.ineligibleForScaleEntityIds())
                .thenReturn(Sets.newHashSet(1L, 2L, 3L, 24L));
        controllableManager.applyScaleEligibility(topologyGraph.getTopologyGraph());
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
        // This is false because on this entity has VM entity type.
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
        assertFalse(dbsEntityBuilder.getAnalysisSettings().getIsEligibleForScale());
    }

    /**
     * Test application of resized down.
     */
    @Test
    public void testApplyResizeDownEligibility() {
        when(entityActionDao.ineligibleForResizeDownEntityIds())
                .thenReturn(Sets.newHashSet(1L, 2L, 3L));
        controllableManager.applyResizable(topologyGraph.getTopologyGraph());
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
        controllableManager.applyResizable(topologyGraph.getTopologyGraph());
        for (CommoditySoldView commSold : vmMaintenance1.getCommoditySoldListList()) {
            assertFalse(commSold.getIsResizeable());
        }
        for (CommoditySoldView commSold : vmMaintenance2.getCommoditySoldListList()) {
            assertFalse(commSold.getIsResizeable());
        }
        // vmBazEntityBuilder is not on maintenance host
        for (CommoditySoldView commSold : vmBazEntityBuilder.getCommoditySoldListList()) {
            assertTrue(commSold.getIsResizeable());
        }
    }

    /**
     * Test if storages in maintenance are set to not controllable.
     */
    @Test
    public void testMarkStoragesOnMaintenanceAsNotResizable() {
        int numModified = controllableManager.applyControllable(topologyGraph);

        assertFalse(storageInMaintenance.getAnalysisSettings().getControllable());
    }

    /**
     * Test hosts exit maintenance mode.
     */
    @Test
    public void testKeepControllableFalseAfterExitingMaintenanceMode() {
        final long windowMin = 20L;
        long defaultId = 12L;
        // user settings but no default policy
        topologyGraph = new GraphWithSettings(TopologyEntityTopologyGraphCreator.newGraph(topology),
                        ImmutableMap.of(pmPoweredOn.getOid(), createDrsWindowEntitySettings(windowMin, defaultId),
                                        pmFailover.getOid(), createDrsWindowEntitySettings(windowMin * 2, defaultId)),
                        Collections.singletonMap(defaultId, SettingPolicy.newBuilder().build()));
        // pmPoweredOn is out of the drs window, pmFailover is still in
        when(entityMaintenanceTimeDao.getHostsThatLeftMaintenance())
                        .thenReturn(ImmutableMap.of(pmPoweredOn.getOid(), (long)(NOW_MS * Units.MILLI - windowMin * 2 * 60),
                                        pmFailover.getOid(), (long)((NOW_MS * Units.MILLI - windowMin * 60))));
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(5, numModified);
        assertTrue(pmPoweredOn.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance.getAnalysisSettings().getControllable());
        assertFalse(pmFailover.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
    }

    /**
     * Test that default drs protection window value applies if there is no user policy.
     */
    @Test
    public void testKeepControllableFalseAfterExitingMaintenanceModeDefaultDrsSetting() {
        // default policy but no user settings
        long defaultWindow = 40L;
        SettingPolicy defaultPolicy = SettingPolicy.newBuilder()
                        .setInfo(SettingPolicyInfo.newBuilder().addSettings(
                                        createSingleDrsWindowSetting(40L)).build())
                        .build();
        topologyGraph = new GraphWithSettings(TopologyEntityTopologyGraphCreator.newGraph(topology),
                        Collections.emptyMap(),
                        Collections.singletonMap(1L, defaultPolicy));
        // pmPoweredOn is out of the drs window
        when(entityMaintenanceTimeDao.getHostsThatLeftMaintenance()).thenReturn(ImmutableMap.of(
                        pmPoweredOn.getOid(),
                        (long)(NOW_MS * Units.MILLI - defaultWindow * Units.MINUTE - 1)));
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(4, numModified);
        assertTrue(pmPoweredOn.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance.getAnalysisSettings().getControllable());
        assertTrue(pmFailover.getAnalysisSettings().getControllable());
        assertFalse(pmInMaintenance2.getAnalysisSettings().getControllable());
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
    }

    private static Setting createSingleDrsWindowSetting(long value) {
        return Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.DrsMaintenanceProtectionWindow.getSettingName())
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value))
                        .build();
    }

    private static EntitySettings createDrsWindowEntitySettings(long value, long defaultId) {
        return EntitySettings.newBuilder().addUserSettings(SettingToPolicyId.newBuilder()
            .setSetting(createSingleDrsWindowSetting(value)))
            .setDefaultSettingPolicyId(defaultId)
            .build();
    }

    private static GraphWithSettings createNoSettingsGraph(Map<Long, TopologyEntity.Builder> topology) {
        return new GraphWithSettings(TopologyEntityTopologyGraphCreator.newGraph(topology),
                        Collections.emptyMap(), Collections.emptyMap());
    }
}
