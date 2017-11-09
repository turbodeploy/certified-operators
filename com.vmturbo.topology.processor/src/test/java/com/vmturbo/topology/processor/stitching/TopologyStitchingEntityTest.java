package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class TopologyStitchingEntityTest {

    private final long targetId = 1234L;

    private final TopologyStitchingEntity pm = new TopologyStitchingEntity(physicalMachine("pm")
        .selling(cpuMHz().capacity(100.0))
        .build().toBuilder(), 1L, targetId);

    private final TopologyStitchingEntity vm = new TopologyStitchingEntity(virtualMachine("vm")
        .buying(cpuMHz().from("pm").used(50.0))
        .build().toBuilder(), 2L, targetId);

    @Before
    public void setup() {
        vm.putProviderCommodities(pm, vm.getEntityBuilder().getCommoditiesBought(0).getBoughtList());
    }

    @Test
    public void testGetProviders() {
        assertEquals(
            vm.getEntityBuilder().getCommoditiesBought(0).getBoughtList(),
            vm.getProviderCommodities(pm).get());
    }

    @Test
    public void testHasProvider() {
        assertTrue(vm.hasProvider(pm));
    }

    @Test
    public void testGetTargetId() {
        assertEquals(targetId, vm.getTargetId());
    }

    @Test
    public void testGetOid() {
        assertEquals(1L, pm.getOid());
        assertEquals(2L, vm.getOid());
    }
}