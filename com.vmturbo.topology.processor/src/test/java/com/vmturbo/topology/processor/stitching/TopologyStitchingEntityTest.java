package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static com.vmturbo.platform.common.builders.EntityBuilders.virtualMachine;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

public class TopologyStitchingEntityTest {

    private final long targetId = 1234L;

    private final TopologyStitchingEntity pm = new TopologyStitchingEntity(physicalMachine("pm")
        .selling(cpuMHz().capacity(100.0))
        .build().toBuilder(), 1L, targetId, 1000L);

    private final TopologyStitchingEntity vm = new TopologyStitchingEntity(virtualMachine("vm")
        .buying(cpuMHz().from("pm").used(50.0))
        .build().toBuilder(), 2L, targetId, 0);

    @Before
    public void setup() {
        vm.putProviderCommodities(pm, vm.getEntityBuilder()
            .getCommoditiesBought(0).getBoughtList().stream()
            .map(CommodityDTO::toBuilder)
            .collect(Collectors.toList()));
    }

    @Test
    public void testGetProviders() {
        assertEquals(
            vm.getEntityBuilder().getCommoditiesBought(0).getBoughtList(),
            vm.getProviderCommodities(pm).get().stream()
                .map(CommodityDTO.Builder::build)
                .collect(Collectors.toList())
        );
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

    @Test
    public void testSetCommoditiesSold() {
        vm.addCommoditySold(CommodityDTO.newBuilder().setCommodityType(CommodityType.VCPU), Optional.empty());

        final CommodityDTO.Builder vmem = CommodityDTO.newBuilder().setCommodityType(CommodityType.VMEM);
        final CommodityDTO.Builder vstorage = CommodityDTO.newBuilder().setCommodityType(CommodityType.VSTORAGE);

        vm.setCommoditiesSold(Arrays.asList(
            new CommoditySold(vmem, null),
            new CommoditySold(vstorage, null)
        ));

        assertThat(vm.getCommoditiesSold().collect(Collectors.toList()), contains(vmem, vstorage));
    }

    @Test
    public void testMergeFromTargetIds() {
        assertThat(pm.getMergeInformation(), is(empty()));

        pm.addMergeInformation(new StitchingMergeInformation(999L, 123456L));

        assertEquals(1, pm.getMergeInformation().size());
        assertEquals(123456L, pm.getMergeInformation().get(0).getTargetId());
        assertEquals(999L, pm.getMergeInformation().get(0).getOid());
    }

    @Test
    public void testMergeFromTargetIdsDuplicate() {
        assertThat(pm.getMergeInformation(), is(empty()));

        pm.addMergeInformation(new StitchingMergeInformation(999L, 123456L));
        pm.addMergeInformation(new StitchingMergeInformation(999L, 123456L));

        assertEquals(1, pm.getMergeInformation().size());
    }

    @Test
    public void testAddAllMergeFromTargetIds() {
        pm.addAllMergeInformation(Arrays.asList(new StitchingMergeInformation(999L, 123L),
            new StitchingMergeInformation(999L, 456L)));

        assertThat(pm.getMergeInformation(), containsInAnyOrder(new StitchingMergeInformation(999L, 123L),
            new StitchingMergeInformation(999L, 456L)));
    }

    @Test
    public void testDiscoveryOrigin() {
        assertThat(pm.buildDiscoveryOrigin().getDiscoveringTargetIdsList(), contains(pm.getTargetId()));

        pm.addMergeInformation(new StitchingMergeInformation(123L, 5555L));
        assertThat(pm.buildDiscoveryOrigin().getDiscoveringTargetIdsList(),
            containsInAnyOrder(pm.getTargetId(), 5555L));
    }

    @Test
    public void testDiscoveryOriginDuplicateTargets() {
        pm.addMergeInformation(new StitchingMergeInformation(999L, pm.getTargetId()));
        pm.addMergeInformation(new StitchingMergeInformation(456L, pm.getTargetId()));

        // Merging information from multiple entities for the same target down to a single entity
        // (shouldn't happen) should only result in a single targetId in the list.
        assertThat(pm.buildDiscoveryOrigin().getDiscoveringTargetIdsList(), contains(pm.getTargetId()));
    }

    @Test
    public void testUpdateTimeOlder() {
        assertEquals(1000L, pm.getLastUpdatedTime());

        pm.updateLastUpdatedTime(0);
        assertEquals(1000L, pm.getLastUpdatedTime());
    }

    @Test
    public void testUpdateTimeNewer() {
        assertEquals(1000L, pm.getLastUpdatedTime());

        pm.updateLastUpdatedTime(2000L);
        assertEquals(2000L, pm.getLastUpdatedTime());
    }
}