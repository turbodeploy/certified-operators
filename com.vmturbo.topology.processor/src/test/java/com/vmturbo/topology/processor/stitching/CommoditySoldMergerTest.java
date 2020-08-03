package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.EntityBuilders.physicalMachine;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.stitching.DataDrivenStitchingOperation.MetaDataAwareMergeCommoditySoldStrategy;
import com.vmturbo.stitching.utilities.MergeEntities;
import com.vmturbo.stitching.utilities.MergeEntities.MergeCommoditySoldStrategy;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

public class CommoditySoldMergerTest {

    final CommoditySoldMerger merger = new CommoditySoldMerger(MergeEntities.KEEP_DISTINCT_FAVOR_ONTO);
    private final TopologyStitchingEntity pm = new TopologyStitchingEntity(physicalMachine("pm")
        .build().toBuilder(), 1L, 2L, 0);

    final MergeCommoditySoldStrategy dropAllStrategy = new MergeCommoditySoldStrategy() {
        @Nonnull
        @Override
        public Optional<Builder> onDistinctCommodity(@Nonnull Builder commodity, Origin origin) {
            return Optional.empty();
        }

        @Nonnull
        @Override
        public Optional<Builder> onOverlappingCommodity(@Nonnull Builder fromCommodity, @Nonnull Builder ontoCommodity) {
            return Optional.empty();
        }
    };

    final CommoditySold vcpuA = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.VCPU)
        .setCapacity(100.0)
        .setKey("A"), null);
    final CommoditySold vcpuABigger = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.VCPU)
        .setCapacity(200.0)
        .setKey("A"), null);

    final CommoditySold vcpuB = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.VCPU)
        .setKey("B"), pm);
    final CommoditySold vcpuNoKey = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.VCPU), null);
    final CommoditySold vmemA = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.VMEM)
        .setKey("A"), null);

    final CommoditySold appA = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.APPLICATION)
        .setKey("A"), null);
    final CommoditySold appB = new CommoditySold(CommodityDTO.newBuilder()
        .setCommodityType(CommodityType.APPLICATION)
        .setKey("B"), null);

    @Test
    public void testMergeCommodityDistinctFrom() {
        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.singletonList(vcpuA), Collections.emptyList());

        assertThat(merged, contains(vcpuA));
    }

    @Test
    public void testMergeCommodityDistinctOnto() {
        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.emptyList(), Collections.singletonList(vcpuA));

        assertThat(merged, contains(vcpuA));
    }

    @Test
    public void testMergeCommodityDistinctFromAndOnto() {
        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.singletonList(vcpuA), Collections.singletonList(vcpuB));

        assertThat(merged, containsInAnyOrder(vcpuA, vcpuB));
    }

    @Test
    public void testMergeCommodityNoKeyDistinctFromKey() {
        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.singletonList(vcpuNoKey), Collections.singletonList(vcpuA));

        assertThat(merged, containsInAnyOrder(vcpuA, vcpuNoKey));
    }

    @Test
    public void testMergeCommodityDistinctOverlapping() {
        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Arrays.asList(vcpuA, vcpuB), Arrays.asList(vcpuABigger, vmemA));

        assertThat(merged, containsInAnyOrder(vcpuABigger, vcpuB, vmemA));
    }

    @Test
    public void testDropFrom() {
        final CommoditySoldMerger merger = new CommoditySoldMerger(dropAllStrategy);

        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.singletonList(vcpuA), Collections.emptyList());

        assertThat(merged, is(empty()));
    }

    @Test
    public void testDropOnto() {
        final CommoditySoldMerger merger = new CommoditySoldMerger(dropAllStrategy);

        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.emptyList(), Collections.singletonList(vcpuA));

        assertThat(merged, is(empty()));
    }

    @Test
    public void testDropOverlap() {
        final CommoditySoldMerger merger = new CommoditySoldMerger(dropAllStrategy);

        final List<CommoditySold> merged = merger.mergeCommoditiesSold(
            Collections.singletonList(vcpuA), Collections.singletonList(vcpuABigger));

        assertThat(merged, is(empty()));
    }

    @Test
    public void testMergeSoldCommoditiesIgnoreIfPresent() {
        // create a commodity sold metadata, both vcpu and application commodity will be merged
        final MetaDataAwareMergeCommoditySoldStrategy mergeStrategy =
            new MetaDataAwareMergeCommoditySoldStrategy(ImmutableList.of(
                CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.VCPU).build(),
                CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.APPLICATION).build()));

        CommoditySoldMerger merger = new CommoditySoldMerger(mergeStrategy);

        List<CommoditySold> fromCommodities = ImmutableList.of(vcpuABigger, appA);
        List<CommoditySold> ontoCommodities = ImmutableList.of(vcpuA, appB);

        List<CommoditySold> merged = merger.mergeCommoditiesSold(fromCommodities, ontoCommodities);
        // appA is merged and both appA and appB are in final list since they have different key
        assertThat(merged, containsInAnyOrder(vcpuA, appA, appB));

        // create commodity sold metadata, the application commodity will be ignored if present
        final MetaDataAwareMergeCommoditySoldStrategy mergeStrategyIgnoreIfPresent =
            new MetaDataAwareMergeCommoditySoldStrategy(ImmutableList.of(
                CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.VCPU).build(),
                CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.APPLICATION)
                    .setIgnoreIfPresent(true).build()));
        merger = new CommoditySoldMerger(mergeStrategyIgnoreIfPresent);
        merged = merger.mergeCommoditiesSold(fromCommodities, ontoCommodities);
        // appA is not merged since there is already an Application commodity (appB) on onto list
        assertThat(merged, containsInAnyOrder(vcpuA, appB));
    }
}