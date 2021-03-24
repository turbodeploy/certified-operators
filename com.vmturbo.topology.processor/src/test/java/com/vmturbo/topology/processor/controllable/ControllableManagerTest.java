package com.vmturbo.topology.processor.controllable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class ControllableManagerTest {

    private final EntityActionDao entityActionDao = Mockito.mock(EntityActionDao.class);

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
                TopologyEntityDTO.CommoditiesBoughtFromProvider.newBuilder().setProviderId(5));
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

    private final Map<Long, Builder> topology = new HashMap<>();

    private TopologyGraph<TopologyEntity> topologyGraph;

    @Before
    public void setup() {
        controllableManager = new ControllableManager(entityActionDao);
        topology.put(vmFooEntityBuilder.getOid(), TopologyEntity.newBuilder(vmFooEntityBuilder));
        topology.put(vmBarEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBarEntityBuilder));
        topology.put(vmBazEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBazEntityBuilder));
        topology.put(pmInMaintenance.getOid(), TopologyEntity.newBuilder(pmInMaintenance));
        topology.put(pmPoweredOn.getOid(), TopologyEntity.newBuilder(pmPoweredOn));

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
        Mockito.when(entityActionDao.getNonControllableEntityIds())
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
        Mockito.when(entityActionDao.getNonControllableEntityIds())
            .thenReturn(Collections.emptySet());
        int numModified = controllableManager.applyControllable(topologyGraph);
        assertEquals(2, numModified);
        assertTrue(container1.getAnalysisSettings().getControllable());
        assertTrue(pod1.getAnalysisSettings().getControllable());
        assertTrue(pod2.getAnalysisSettings().getControllable());
        assertFalse(vm1.getAnalysisSettings().getControllable());
        assertFalse(vm2.getAnalysisSettings().getControllable());
        assertTrue(VDC.getAnalysisSettings().getControllable());
        assertTrue(pmFailover.getAnalysisSettings().getControllable());
    }

    /**
     * Test application of scale.
     */
    @Test
    public void testApplyResize() {
        Mockito.when(entityActionDao.ineligibleForScaleEntityIds())
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
    public void testApplyResizeDown() {
        Mockito.when(entityActionDao.ineligibleForResizeDownEntityIds())
                .thenReturn(Sets.newHashSet(1L, 2L, 3L));
        controllableManager.applyResizeDownEligibility(topologyGraph);
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
        // This is true because on this entity has VM entity type.
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
    }
}
