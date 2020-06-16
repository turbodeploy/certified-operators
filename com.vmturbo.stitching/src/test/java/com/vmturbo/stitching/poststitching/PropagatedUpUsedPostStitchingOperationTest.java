package com.vmturbo.stitching.poststitching;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.STORAGE_ACCESS;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBoughtBuilder;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Test class for PropagatedUpUsedPostStitchingOperation.
 */
public class PropagatedUpUsedPostStitchingOperationTest {
    private static final double EPSILON = 1e-6;

    private final CommodityBoughtDTO.Builder storageAccessBought = makeCommodityBoughtBuilder(STORAGE_ACCESS);
    private final CommodityBoughtDTO.Builder storageLatencyBought = makeCommodityBoughtBuilder(CommodityType.STORAGE_LATENCY);

    private final CommoditySoldDTO storageAccessSold = makeCommoditySold(STORAGE_ACCESS);
    private final CommoditySoldDTO storageLatencySold = makeCommoditySold(CommodityType.STORAGE_LATENCY);

    private TopologyEntity.Builder diskArray = makeBasicTopologyEntityBuilder(1, EntityType.DISK_ARRAY);

    private TopologyEntity.Builder storage1 = makeBasicTopologyEntityBuilder(11, EntityType.STORAGE);
    private TopologyEntity.Builder storage2 = makeBasicTopologyEntityBuilder(12, EntityType.STORAGE);

    private final IStitchingJournal<TopologyEntity> stitchingJournal =
            (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private static final double ACCESS_SOLD_USED = 165;

    private final ImmutableMap<TopologyEntity.Builder, ImmutableMap<Builder, Double>> storageAccessExpectations =
            ImmutableMap.<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>>builder()
                    .put(storage1, ImmutableMap.of(diskArray, 25.0))
                    .put(storage2, ImmutableMap.of(diskArray, 80.0))
                    .build();

    private UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    private void setCommoditySoldUsed(@Nonnull final TopologyEntity.Builder entity,
                                                      final CommodityType commodityType, double used) {
        entity.getEntityBuilder().getCommoditySoldListBuilderList().stream()
                .filter(commodity -> commodity.getCommodityType().getType() == commodityType.getNumber())
                .findFirst()
                .get().setUsed(used);
    }

    private TopologyEntity.Builder makeBasicTopologyEntityBuilder(final long oid, @Nonnull final EntityType type) {
        return makeTopologyEntityBuilder(
                oid, type.getNumber(), Arrays.asList(storageAccessSold, storageLatencySold),
                Arrays.asList(storageAccessBought.build(), storageLatencyBought.build())
        );
    }

    /**
     * test Storage StorageAccess commodity PropagatedUpUsedPostStitchingOperation from DiskArray.
     */
    @Test
    public void testStorageAccessPropagatedUpUsedPostStitchingOperation() {
        final PropagatedUpUsedPostStitchingOperation op =
                new PropagatedUpUsedPostStitchingOperation(EntityType.STORAGE, STORAGE_ACCESS);
        storageAccessExpectations.forEach((entity, relationships) ->
                relationships.forEach((provider, amount) -> {
                    entity.addProvider(provider);
                    provider.addConsumer(entity);
                    setCommoditySoldUsed(provider, STORAGE_ACCESS, ACCESS_SOLD_USED);
                    final TopologyEntity ent = entity.build();
                    op.performOperation(Stream.of(ent), settingsMock, resultBuilder);
                    resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));
                    final CommoditySoldDTO.Builder comm = ent.getTopologyEntityDtoBuilder()
                            .getCommoditySoldListBuilderList().stream()
                            .filter(c -> STORAGE_ACCESS.getNumber() == c.getCommodityType().getType())
                            .findFirst().orElse(null);
                    Assert.assertEquals(ACCESS_SOLD_USED, comm.getUsed(), EPSILON);
                }));
    }
}
