package com.vmturbo.topology.processor.controllable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

public class ControllableManagerTest {

    private final EntityActionDao entityActionDao = Mockito.mock(EntityActionDao.class);

    private ControllableManager controllableManager;

    private final TopologyEntityDTO.Builder vmFooEntityBuilder = TopologyEntityDTO.newBuilder()
            .setOid(1)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setControllable(true));
    private final TopologyEntityDTO.Builder vmBarEntityBuilder = TopologyEntityDTO.newBuilder()
            .setOid(2)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setControllable(true));
    private final TopologyEntityDTO.Builder vmBazEntityBuilder = TopologyEntityDTO.newBuilder()
            .setOid(3)
            .setAnalysisSettings(AnalysisSettings.newBuilder()
                    .setControllable(true));
    private final Map<Long, Builder> topology = new HashMap<>();
    @Before
    public void setup() {
        controllableManager = new ControllableManager(entityActionDao);
        topology.put(vmFooEntityBuilder.getOid(), TopologyEntity.newBuilder(vmFooEntityBuilder));
        topology.put(vmBarEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBarEntityBuilder));
        topology.put(vmBazEntityBuilder.getOid(), TopologyEntity.newBuilder(vmBazEntityBuilder));
    }

    @Test
    public void testApplyControllable() {
        Mockito.when(entityActionDao.getNonControllableEntityIds())
                .thenReturn(Sets.newHashSet(1L));
        controllableManager.applyControllable(topology);
        assertFalse(vmFooEntityBuilder.getAnalysisSettings().getControllable());
        assertTrue(vmBarEntityBuilder.getAnalysisSettings().getControllable());
        assertTrue(vmBazEntityBuilder.getAnalysisSettings().getControllable());
    }
}
