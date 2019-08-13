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

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyEntityTest {

    private static final long ENTITY_OID = 23345;
    private static final int ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private final TopologyEntityDTO.Builder dtoBuilder =
        TopologyEntityDTO.newBuilder().setOid(ENTITY_OID).setEntityType(ENTITY_TYPE);

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
        assertEquals(111L, entity.getDiscoveryOrigin().get().getDiscoveringTargetIds(0));
        assertEquals(222L, entity.getDiscoveryOrigin().get().getLastUpdatedTime());
    }

    @Test
    public void testBuildDiscoveryInformationWithMergeFromTargetIds() {
        final TopologyEntity entity = TopologyEntity.newBuilder(dtoBuilder.setOrigin(
            Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L)
                .withMergeFromTargetIds(333L, 444L)
                .lastUpdatedAt(222L))))
            .build();

        assertTrue(entity.hasDiscoveryOrigin());
        assertThat(entity.getDiscoveryOrigin().get().getDiscoveringTargetIdsList(),
            containsInAnyOrder(111L, 333L, 444L));
        assertEquals(222L, entity.getDiscoveryOrigin().get().getLastUpdatedTime());
    }

    @Test
    public void testSnapshot() {
        final TopologyEntity.Builder diskArray = makeStorageEntity(1, EntityType.DISK_ARRAY, Optional.empty());
        final TopologyEntity.Builder logicalPool = makeStorageEntity(2, EntityType.LOGICAL_POOL, Optional.of(diskArray));
        final TopologyEntity.Builder storage = makeStorageEntity(11, EntityType.STORAGE, Optional.of(logicalPool));
        makeStorageEntity(21, EntityType.VIRTUAL_MACHINE, Optional.of(storage)); // VM buying from storage

        logicalPool.getEntityBuilder()
            .getOriginBuilder()
            .setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L)
                .withMergeFromTargetIds(333L, 444L)
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
        final TopologyEntity.Builder consumer = TopologyEntity.newBuilder(dtoBuilder.setOrigin(
            Origin.newBuilder().setDiscoveryOrigin(DiscoveryOriginBuilder.discoveredBy(111L)
                .withMergeFromTargetIds(333L, 444L)
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
}