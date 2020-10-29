package com.vmturbo.topology.processor.controllable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    private final Map<Long, Builder> topology = new HashMap<>();

    @Before
    public void setup() {
        controllableManager = new ControllableManager(entityActionDao);
        topology.put(vmFooEntityBuilder.getOid(), TopologyEntity.newBuilder(vmFooEntityBuilder));
        topology.put(vmBarEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBarEntityBuilder));
        topology.put(vmBazEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBazEntityBuilder));
        topology.put(pmInMaintenance.getOid(), TopologyEntity.newBuilder(pmInMaintenance));
        topology.put(pmPoweredOn.getOid(), TopologyEntity.newBuilder(pmPoweredOn));
    }

    /**
     * Test that the correct set of entities have their controllable bit reset given the list of ids
     * returned by the database is correct.
     *
     * <p>Currently it doesn't check for side-effects.</p>
     */
    @Test
    public void testApplyControllable() {
        Mockito.when(entityActionDao.getNonControllableEntityIds())
                .thenReturn(Sets.newHashSet(1L, 3L, 4L));
        controllableManager.applyControllable(topology);
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getControllable());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getControllable());
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getControllable());
        assertTrue(pmInMaintenance.getAnalysisSettings().getControllable());
        assertTrue(pmPoweredOn.getAnalysisSettings().getControllable());
    }

    /**
     * Test application of scale.
     */
    @Test
    public void testApplyResize() {
        Mockito.when(entityActionDao.ineligibleForScaleEntityIds())
                .thenReturn(Sets.newHashSet(1L, 2L, 3L));
        controllableManager.applyScaleEligibility(topology);
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
        controllableManager.applyResizeDownEligibility(topology);
        assertTrue(vmFooEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
        // This is true because on this entity has VM entity type.
        assertFalse(vmBazEntityBuilder.getAnalysisSettings().getIsEligibleForResizeDown());
    }
}
