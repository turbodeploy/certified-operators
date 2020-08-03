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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

public class TopologyStitchingEntityTest {

    private static final long targetId = 1234L;

    private static final String pmLocalName = "qqq";

    private static final TopologyStitchingEntity vm_remove =
        new TopologyStitchingEntity(virtualMachine("vmRemove").build().toBuilder()
            .setOrigin(EntityOrigin.PROXY)
        .setKeepStandalone(false), 3L, targetId, 0);

    private TopologyStitchingEntity pm;
    private TopologyStitchingEntity vm;

    @Before
    public void setup() {
        pm = new TopologyStitchingEntity(physicalMachine("pm")
             .selling(cpuMHz().capacity(100.0)).property(SupplyChainConstants.LOCAL_NAME, pmLocalName)
             .build().toBuilder(), 1L, targetId, 1000L);
        vm = new TopologyStitchingEntity(virtualMachine("vm")
              .buying(cpuMHz().from("pm").used(50.0))
              .build().toBuilder().setOrigin(EntityOrigin.PROXY), 2L, targetId, 0);
        vm.addProviderCommodityBought(pm, new CommoditiesBought(vm.getEntityBuilder()
            .getCommoditiesBought(0).getBoughtList().stream()
            .map(CommodityDTO::toBuilder)
            .collect(Collectors.toList())));
    }

    @Test
    public void testGetProviders() {
        assertEquals(
            vm.getEntityBuilder().getCommoditiesBought(0).getBoughtList(),
            vm.getCommodityBoughtListByProvider().get(pm).stream()
                    .flatMap(cb -> cb.getBoughtList().stream())
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

        pm.addMergeInformation(new StitchingMergeInformation(999L, 123456L, StitchingErrors.none()));

        assertEquals(1, pm.getMergeInformation().size());
        assertEquals(123456L, pm.getMergeInformation().get(0).getTargetId());
        assertEquals(999L, pm.getMergeInformation().get(0).getOid());
    }

    @Test
    public void testMergeFromTargetIdsDuplicate() {
        assertThat(pm.getMergeInformation(), is(empty()));

        pm.addMergeInformation(new StitchingMergeInformation(999L, 123456L, StitchingErrors.none()));
        pm.addMergeInformation(new StitchingMergeInformation(999L, 123456L, StitchingErrors.none()));

        assertEquals(1, pm.getMergeInformation().size());
    }

    @Test
    public void testAddAllMergeFromTargetIds() {
        pm.addAllMergeInformation(Arrays.asList(new StitchingMergeInformation(999L, 123L, StitchingErrors.none()),
            new StitchingMergeInformation(999L, 456L, StitchingErrors.none())));

        assertThat(pm.getMergeInformation(), containsInAnyOrder(new StitchingMergeInformation(999L, 123L, StitchingErrors.none()),
            new StitchingMergeInformation(999L, 456L, StitchingErrors.none())));
    }

    @Test
    public void testHasMergeInformation() {
        assertFalse(pm.hasMergeInformation());

        pm.addMergeInformation(new StitchingMergeInformation(999L, 123L, StitchingErrors.none()));
        assertTrue(pm.hasMergeInformation());
    }

    @Test
    public void testDiscoveryOrigin() {
        Map<Long, PerTargetEntityInformation> target2name = pm.buildDiscoveryOrigin()
                        .getDiscoveredTargetDataMap();
        assertThat(target2name.keySet(), contains(pm.getTargetId()));
        assertEquals(pmLocalName, target2name.get(targetId).getVendorId());

        String localName2 = "www";
        long targetId2 = 5555L;
        pm.addMergeInformation(new StitchingMergeInformation(123L, targetId2, StitchingErrors.none(), localName2));
        target2name = pm.buildDiscoveryOrigin().getDiscoveredTargetDataMap();
        assertThat(target2name.keySet(), containsInAnyOrder(pm.getTargetId(), targetId2));
        assertEquals(pmLocalName, target2name.get(targetId).getVendorId());
        assertEquals(localName2, target2name.get(targetId2).getVendorId());
    }

    @Test
    public void testDiscoveryOriginDuplicateTargets() {
        pm.addMergeInformation(new StitchingMergeInformation(999L, pm.getTargetId(), StitchingErrors.none()));
        pm.addMergeInformation(new StitchingMergeInformation(456L, pm.getTargetId(), StitchingErrors.none()));

        // Merging information from multiple entities for the same target down to a single entity
        // (shouldn't happen) should only result in a single targetId in the list.
        assertThat(pm.buildDiscoveryOrigin().getDiscoveredTargetDataMap().keySet(),
                   contains(pm.getTargetId()));
    }

    @Test
    public void testCombinedErrors() {
        final StitchingErrors stitchingErrors1 = new StitchingErrors();
        stitchingErrors1.add(StitchingErrorCode.INVALID_COMM_BOUGHT);

        final StitchingErrors stitchingErrors2 = new StitchingErrors();
        stitchingErrors2.add(StitchingErrorCode.INCONSISTENT_KEY);

        pm.recordError(StitchingErrorCode.INVALID_COMM_SOLD);
        pm.addMergeInformation(new StitchingMergeInformation(999L, pm.getTargetId(), stitchingErrors1));
        pm.addMergeInformation(new StitchingMergeInformation(456L, pm.getTargetId(), stitchingErrors2));

        final StitchingErrors combinedErrors = pm.combinedEntityErrors();
        assertTrue(combinedErrors.contains(StitchingErrorCode.INVALID_COMM_SOLD,
            StitchingErrorCode.INVALID_COMM_BOUGHT, StitchingErrorCode.INCONSISTENT_KEY));
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

    @Test
    public void testSnapshot() {
        pm.addMergeInformation(new StitchingMergeInformation(9911L, 9090L, StitchingErrors.none()));
        final TopologyStitchingEntity snapshotCopy = (TopologyStitchingEntity)pm.snapshot();

        // Should be comparison equal but not reference equal
        assertEquals(pm.getCommodityBoughtListByProvider(), snapshotCopy.getCommodityBoughtListByProvider());
        assertFalse(pm.getCommodityBoughtListByProvider() == snapshotCopy.getCommodityBoughtListByProvider());

        // Should be comparison equal but not reference equal
        assertEquals(pm.getTopologyCommoditiesSold(), snapshotCopy.getTopologyCommoditiesSold());
        assertFalse(pm.getTopologyCommoditiesSold() == snapshotCopy.getTopologyCommoditiesSold());

        // Should be comparison equal but not reference equal
        assertEquals(pm.getMergeInformation(), snapshotCopy.getMergeInformation());
        assertFalse(pm.getMergeInformation() == snapshotCopy.getMergeInformation());

        // Built versions should be comparison equal but builders should not be reference equal
        assertEquals(pm.getEntityBuilder().build(), snapshotCopy.getEntityBuilder().build());
        assertFalse(pm.getEntityBuilder() == snapshotCopy.getEntityBuilder());

        assertEquals(pm.getLastUpdatedTime(), snapshotCopy.getLastUpdatedTime());
        assertEquals(pm.getTargetId(), snapshotCopy.getTargetId());
        assertEquals(pm.removeIfUnstitched(), snapshotCopy.removeIfUnstitched());
    }

    /**
     * Tests that entities with keepStandalone == false and origin == PROXY get marked with
     * removeIfUnstitched = true, but other entities do not.
     */
    @Test
    public void testRemoveIfUnstitched() {
        assertFalse(vm.removeIfUnstitched());
        assertFalse(pm.removeIfUnstitched());
        assertTrue(vm_remove.removeIfUnstitched());
    }
}