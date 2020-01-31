package com.vmturbo.stitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBoughtBuilder;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyEntityTest {

    private static final long ENTITY_OID = 23345;
    private static final int ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private final TopologyEntityDTO.Builder dtoBuilder =
        TopologyEntityDTO.newBuilder().setOid(ENTITY_OID).setEntityType(ENTITY_TYPE);

    /**
     * Builders for a simple cloud topology: region owns zone aggregates VM connects to Volume.
     */
    private TopologyEntity.Builder vmBuilder;
    private TopologyEntity.Builder azBuilder;
    private TopologyEntity.Builder rgBuilder;
    private TopologyEntity.Builder volBuilder;

    @Test
    public void testBuild() {
        final TopologyEntity entity = TopologyEntity.newBuilder(dtoBuilder).build();

        assertEquals(ENTITY_OID, entity.getOid());
        assertEquals(ENTITY_TYPE, entity.getEntityType());
        assertEquals(dtoBuilder, entity.getTopologyEntityDtoBuilder());
        assertFalse(entity.hasDiscoveryOrigin());
    }

    @Test
    public void testBuildDiscoveryInformation() {
        final TopologyEntity entity = TopologyEntity.newBuilder(dtoBuilder.setOrigin(
                Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L).lastUpdatedAt(222L)))
        ).build();

        assertTrue(entity.hasDiscoveryOrigin());
        DiscoveryOrigin origin = entity.getDiscoveryOrigin().get();
        assertEquals(111L, origin.getDiscoveredTargetDataMap().entrySet().iterator().next().getKey().longValue());
        assertEquals(222L, origin.getLastUpdatedTime());
    }

    @Test
    public void testBuildDiscoveryInformationWithMergeFromTargetIds() {
        final StitchingMergeInformation mergedTarget1 =
                        new StitchingMergeInformation(1L, 333L,
                                                      StitchingErrors.none());
        final StitchingMergeInformation mergedTarget2 =
                    new StitchingMergeInformation(1L, 444L,
                                                  StitchingErrors.none());
        final TopologyEntity entity = TopologyEntity.newBuilder(dtoBuilder.setOrigin(
            Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L)
                .withMerge(mergedTarget1, mergedTarget2)
                .lastUpdatedAt(222L))))
            .build();

        assertTrue(entity.hasDiscoveryOrigin());
        assertThat(entity.getDiscoveryOrigin().get().getDiscoveredTargetDataMap().keySet(),
            containsInAnyOrder(111L, mergedTarget1.getTargetId(), mergedTarget2.getTargetId()));
        assertEquals(222L, entity.getDiscoveryOrigin().get().getLastUpdatedTime());
    }

    @Test
    public void testSnapshot() {
        final TopologyEntity.Builder diskArray = makeStorageEntity(1, EntityType.DISK_ARRAY, Optional.empty());
        final TopologyEntity.Builder logicalPool = makeStorageEntity(2, EntityType.LOGICAL_POOL, Optional.of(diskArray));
        final TopologyEntity.Builder storage = makeStorageEntity(11, EntityType.STORAGE, Optional.of(logicalPool));
        makeStorageEntity(21, EntityType.VIRTUAL_MACHINE, Optional.of(storage)); // VM buying from storage

        final StitchingMergeInformation mergedTarget1 =
                        new StitchingMergeInformation(logicalPool.getOid(), 333L,
                                                      StitchingErrors.none());
        final StitchingMergeInformation mergedTarget2 =
                    new StitchingMergeInformation(logicalPool.getOid(), 444L,
                                                  StitchingErrors.none());
        logicalPool.getEntityBuilder()
            .getOriginBuilder()
            .setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L)
                .withMerge(mergedTarget1, mergedTarget2)
                .lastUpdatedAt(222L));

        final TopologyEntity da = diskArray.build();
        final TopologyEntity lp = da.getConsumers().get(0);
        assertEquals(logicalPool.getOid(), lp.getOid());
        final TopologyEntity snapshotCopy = lp.snapshot();

        // Should be comparison equal but not reference equal
        assertThat(lp.getConsumers(), not(empty()));
        assertEquals(lp.getConsumers(), snapshotCopy.getConsumers());
        assertFalse(lp.getConsumers() == snapshotCopy.getConsumers());

        // Should be comparison equal but not reference equal
        assertThat(lp.getProviders(), not(empty()));
        assertEquals(lp.getProviders(), snapshotCopy.getProviders());
        assertFalse(lp.getProviders() == snapshotCopy.getProviders());

        assertTrue(lp.hasDiscoveryOrigin());
        assertEquals(lp.getOrigin().get(), snapshotCopy.getOrigin().get());

        // Built versions should be comparison equal but builders should not be reference equal
        assertEquals(lp.getTopologyEntityDtoBuilder().build(), snapshotCopy.getTopologyEntityDtoBuilder().build());
        assertFalse(lp.getTopologyEntityDtoBuilder() == snapshotCopy.getTopologyEntityDtoBuilder());
    }

    @Test
    public void testBuilderSnapshot() {
        final TopologyEntity.Builder diskArray = makeStorageEntity(1122, EntityType.DISK_ARRAY,
            Optional.empty());
        final TopologyEntity.Builder logicalPool = makeStorageEntity(2211, EntityType.LOGICAL_POOL, Optional.of(diskArray));
        final TopologyEntity.Builder snapShot = logicalPool.snapshot();
        final TopologyEntity logicalPoolBuilt = logicalPool.build();
        final TopologyEntity snapShotBuilt = snapShot.build();
        assertEquals(logicalPoolBuilt.getProviders(), snapShotBuilt.getProviders());
        assertEquals(logicalPoolBuilt.getOid(), snapShotBuilt.getOid());
        assertFalse(logicalPoolBuilt.getTopologyEntityDtoBuilder() ==
            snapShotBuilt.getTopologyEntityDtoBuilder());
    }

    private TopologyEntity.Builder makeStorageEntity(final long oid, @Nonnull final EntityType type,
                                                     @Nonnull final Optional<TopologyEntity.Builder> provider) {
        final CommoditySoldDTO storageAccessSold = makeCommoditySold(CommodityType.STORAGE_ACCESS);
        final CommoditySoldDTO storageLatencySold = makeCommoditySold(CommodityType.STORAGE_LATENCY);

        final CommodityBoughtDTO.Builder storageAccessBought =
            makeCommodityBoughtBuilder(CommodityType.STORAGE_ACCESS);
        final CommodityBoughtDTO.Builder storageLatencyBought =
            makeCommodityBoughtBuilder(CommodityType.STORAGE_LATENCY);

        final TopologyEntity.Builder entity = makeTopologyEntityBuilder(
            oid, type.getNumber(), Arrays.asList(storageAccessSold, storageLatencySold),
            Collections.emptyList());

        provider.ifPresent(p -> {
            entity.addProvider(p);
            p.addConsumer(entity);

            entity.getEntityBuilder()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(p.getOid())
                    .setProviderEntityType(p.getEntityType())
                    .addCommodityBought(storageAccessBought)
                    .addCommodityBought(storageLatencyBought)
                    .build());
        });

        return entity;
    }

    @Test
    public void testClearConsumersAndProviders() {
        final StitchingMergeInformation mergedTarget1 =
                        new StitchingMergeInformation(1L, 333L,
                                                      StitchingErrors.none());
        final StitchingMergeInformation mergedTarget2 =
                        new StitchingMergeInformation(1L, 444L,
                                                      StitchingErrors.none());
        final TopologyEntity.Builder consumer = TopologyEntity.newBuilder(dtoBuilder.setOrigin(
            Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L)
                .withMerge(mergedTarget1, mergedTarget2)
                .lastUpdatedAt(222L))));
        final TopologyEntity.Builder provider = TopologyEntity.newBuilder(dtoBuilder.setOrigin(
            Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(222L)
                .lastUpdatedAt(456))));

        consumer.addProvider(provider);
        provider.addConsumer(consumer);
        assertEquals(1, consumer.build().getProviders().size());
        assertEquals(1, provider.build().getConsumers().size());

        consumer.clearConsumersAndProviders();
        provider.clearConsumersAndProviders();

        assertEquals(0, consumer.build().getProviders().size());
        assertEquals(0, provider.build().getConsumers().size());
    }

    /**
     * Tests that the connection getters return the appropriate information in a small
     * topology where a region owns a zone which aggregates a VM.
     */
    @Test
    public void testConnections() {
        makeSimpleCloudTopology();

        final TopologyEntity vm = vmBuilder.build();
        final TopologyEntity az = azBuilder.build();
        final TopologyEntity rg = rgBuilder.build();
        final TopologyEntity vol = volBuilder.build();

        assertFalse(vm.getOwner().isPresent());
        assertEquals(0, vm.getOwnedEntities().size());
        assertEquals(1, vm.getAggregators().size());
        assertEquals(vm.getAggregators().get(0), az);
        assertEquals(0, vm.getAggregatedEntities().size());
        assertEquals(0, vm.getAggregatedAndOwnedEntities().size());
        assertEquals(1, vm.getAggregatorsAndOwner().size());
        assertEquals(vm.getAggregatorsAndOwner().get(0), az);

        assertEquals(az.getOwner().get(), rg);
        assertEquals(0, az.getOwnedEntities().size());
        assertEquals(0, az.getAggregators().size());
        assertEquals(1, az.getAggregatedEntities().size());
        assertEquals(az.getAggregatedEntities().get(0), vm);
        assertEquals(1, az.getAggregatedAndOwnedEntities().size());
        assertEquals(az.getAggregatedAndOwnedEntities().get(0), vm);
        assertEquals(1, az.getAggregatorsAndOwner().size());
        assertEquals(az.getAggregatorsAndOwner().get(0), rg);

        assertFalse(rg.getOwner().isPresent());
        assertEquals(1, rg.getOwnedEntities().size());
        assertEquals(rg.getOwnedEntities().get(0), az);
        assertEquals(0, rg.getAggregators().size());
        assertEquals(0, rg.getAggregatedEntities().size());
        assertEquals(1, rg.getAggregatedAndOwnedEntities().size());
        assertEquals(rg.getAggregatedAndOwnedEntities().get(0), az);
        assertEquals(0, rg.getAggregatorsAndOwner().size());

        assertFalse(vol.getOwner().isPresent());
        assertEquals(0, vol.getOwnedEntities().size());
        assertEquals(0, vol.getAggregators().size());
        assertEquals(0, vol.getAggregatedEntities().size());
        assertEquals(0, vol.getAggregatedAndOwnedEntities().size());
        assertEquals(0, vol.getAggregatorsAndOwner().size());
    }

    /**
     * Tests that snapshots work properly in a small
     * topology where a region owns a zone which aggregates a VM.
     */
    @Test
    public void testSnapshotInConnectedTopology() {
        makeSimpleCloudTopology();

        final TopologyEntity vm = vmBuilder.build();
        final TopologyEntity az = azBuilder.build();
        final TopologyEntity vol = volBuilder.build();

        final TopologyEntity vmSnapshot = vm.snapshot();
        assertFalse(vm == vmSnapshot);
        assertEquals(vm.getOid(), vmSnapshot.getOid());
        assertEquals(vm.getDisplayName(), vmSnapshot.getDisplayName());
        assertEquals(vm.getEntityType(), vmSnapshot.getEntityType());


        assertEquals(1, vmSnapshot.getOutboundAssociatedEntities().size());
        assertEquals(vmSnapshot.getOutboundAssociatedEntities().get(0), vol);
        assertFalse(vmSnapshot.getOwner().isPresent());
        assertEquals(0, vmSnapshot.getOwnedEntities().size());
        assertEquals(1, vmSnapshot.getAggregators().size());
    }

    /**
     * Tests that clearing connections work properly.
     */
    @Test
    public void testClearConnections() {
        makeSimpleCloudTopology();

        vmBuilder.clearConsumersAndProviders();
        azBuilder.clearConsumersAndProviders();
        rgBuilder.clearConsumersAndProviders();
        volBuilder.clearConsumersAndProviders();

        assertEntityHasNoConnections(vmBuilder.build());
        assertEntityHasNoConnections(azBuilder.build());
        assertEntityHasNoConnections(rgBuilder.build());
        assertEntityHasNoConnections(volBuilder.build());
    }

    private void makeSimpleCloudTopology() {
        final long vmId = 1L;
        final long azId = 2L;
        final long rgId = 3L;
        final long volId = 4L;

        vmBuilder = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                .setOid(vmId)
                                                .setDisplayName("foo")
                                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE));

        azBuilder = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                .setOid(azId)
                                                .setDisplayName("foo")
                                                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE));

        rgBuilder = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                .setOid(rgId)
                                                .setDisplayName("foo")
                                                .setEntityType(EntityType.REGION_VALUE));
        volBuilder = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                                                .setOid(volId)
                                                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE));

        vmBuilder.addOutboundAssociation(volBuilder);
        volBuilder.addInboundAssociation(vmBuilder);
        vmBuilder.addAggregator(azBuilder);
        azBuilder.addAggregatedEntity(vmBuilder);
        azBuilder.addOwner(rgBuilder);
        rgBuilder.addOwnedEntity(azBuilder);
    }

    private void assertEntityHasNoConnections(TopologyEntity entity) {
        assertFalse(entity.getOwner().isPresent());
        assertEquals(0, entity.getOwnedEntities().size());
        assertEquals(0, entity.getAggregators().size());
        assertEquals(0, entity.getAggregatedEntities().size());
        assertEquals(0, entity.getAggregatedAndOwnedEntities().size());
        assertEquals(0, entity.getAggregatorsAndOwner().size());
    }
}