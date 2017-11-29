package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.coolingDegC;
import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.powerWatts;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.buying;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.newStitchingGraph;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.utilities.MergeEntities;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.MergeEntitiesChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.RemoveEntityChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityAloneChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityRelationshipsChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;

public class TopologyStitchingChangesTest {

    /**
     * 4   5
     *  \ /
     *   2   3
     *    \ /
     *     1
     */
    private final StitchingEntityData entity1Data = stitchingData("1", Collections.emptyList());
    private final StitchingEntityData entity2Data = stitchingData("2", Collections.singletonList("1"));
    private final StitchingEntityData entity3Data = stitchingData("3", Collections.singletonList("1"));
    private final StitchingEntityData entity4Data = stitchingData("4", Collections.singletonList("2"));
    private final StitchingEntityData entity5Data = stitchingData("5", Collections.singletonList("2"));

    private final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
        "1", entity1Data,
        "2", entity2Data,
        "3", entity3Data,
        "4", entity4Data,
        "5", entity5Data
    );

    private final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
    private final TopologyStitchingEntity entity1 = graph.getEntity(entity1Data.getEntityDtoBuilder()).get();
    private final TopologyStitchingEntity entity2 = graph.getEntity(entity2Data.getEntityDtoBuilder()).get();
    private final TopologyStitchingEntity entity3 = graph.getEntity(entity3Data.getEntityDtoBuilder()).get();
    private final TopologyStitchingEntity entity4 = graph.getEntity(entity4Data.getEntityDtoBuilder()).get();
    private final TopologyStitchingEntity entity5 = graph.getEntity(entity5Data.getEntityDtoBuilder()).get();

    private final StitchingContext stitchingContext = mock(StitchingContext.class);

    static {
        IdentityGenerator.initPrefix(0);
    }

    @Test
    public void testRemoveEntityChange() {
        final RemoveEntityChange change = new RemoveEntityChange(stitchingContext, entity1);
        change.applyChange();

        verify(stitchingContext).removeEntity(entity1);
    }

    @Test
    public void testRemoveEntityMultipleTimesDoesNotThrowException() {
        final RemoveEntityChange change = new RemoveEntityChange(stitchingContext, entity1);
        change.applyChange();
        change.applyChange();
    }

    @Test
    public void testUpdateAloneChange() {
        final UpdateEntityAloneChange change = new UpdateEntityAloneChange(entity2,
            e -> e.getEntityBuilder().setDisplayName("foo"));

        assertNotEquals("foo", entity2.getDisplayName());
        change.applyChange();
        assertEquals("foo", entity2.getDisplayName());
    }

    @Test
    public void testRemoveProviderRemovesConsumers() {
        /**
         * 4   5            4   5
         *  \ /              \ /
         *   2   3   -->      2   3
         *    \ /                /
         *     1                1
         */
        assertThat(entity2.getProviders(), contains(entity1));
        assertThat(entity1.getConsumers(), containsInAnyOrder(entity2, entity3));

        new UpdateEntityRelationshipsChange(entity2, toUpdate -> toUpdate.removeProvider(entity1))
            .applyChange();
        assertThat(entity2.getProviders(), is(empty()));
        assertThat(entity1.getConsumers(), contains(entity3));
    }

    @Test
    public void testAddProviderAddsConsumers() {
        /**
         * 4   5            4   5
         *  \ /              \ / \
         *   2   3   -->      2   3
         *    \ /              \ /
         *     1                1
         */
        assertThat(entity5.getProviders(), contains(entity2));
        assertThat(entity3.getConsumers(), is(empty()));

        new UpdateEntityRelationshipsChange(entity5, toUpdate ->
            entity5.putProviderCommodities(entity3, Collections.singletonList(cpuMHz().build().toBuilder())))
            .applyChange();
        assertThat(entity5.getProviders(), containsInAnyOrder(entity2, entity3));
        assertThat(entity3.getConsumers(), contains(entity5));
    }

    @Test
    public void testMergeEntities() {
        /**
         * 2  4          2   4
         * |  |     -->   \ /
         * 1  3            1
         */
        final StitchingEntityData entity1Data = stitchingData("1", Collections.emptyList());
        final StitchingEntityData entity2Data = stitchingData("2", Collections.singletonList("1"));
        final StitchingEntityData entity3Data = stitchingData("3", Collections.emptyList());
        final StitchingEntityData entity4Data = stitchingData("4", Collections.singletonList("3"));

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1Data,
            "2", entity2Data,
            "3", entity3Data,
            "4", entity4Data
        );

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        final TopologyStitchingEntity entity1 = graph.getEntity(entity1Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity2 = graph.getEntity(entity2Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity3 = graph.getEntity(entity3Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity4 = graph.getEntity(entity4Data.getEntityDtoBuilder()).get();

        assertThat(entity4.getProviders(), contains(entity3));
        assertThat(entity3.getConsumers(), contains(entity4));
        assertThat(entity2.getProviders(), contains(entity1));
        assertThat(entity1.getConsumers(), contains(entity2));
        when(stitchingContext.hasEntity(entity1)).thenReturn(true);
        when(stitchingContext.hasEntity(entity3)).thenReturn(true);

        new MergeEntitiesChange(stitchingContext, entity3, entity1,
            new CommoditySoldMerger(MergeEntities.KEEP_DISTINCT_FAVOR_ONTO)).applyChange();

        verify(stitchingContext).removeEntity(entity3);
        assertThat(entity4.getProviders(), contains(entity1));
        assertThat(entity2.getProviders(), contains(entity1));
        assertThat(entity1.getConsumers(), containsInAnyOrder(entity2, entity4));
    }

    // Test that applying a merge on the same two entities multiple times results in a no-op on the second
    // application of the merge.
    @Test
    public void testMergeEntitiesMultipleTimesIsAllowed() {
        /**
         * 2  4          2   4
         * |  |     -->   \ /
         * 1  3            1
         */
        final StitchingEntityData entity1Data = stitchingData("1", Collections.emptyList());
        final StitchingEntityData entity2Data = stitchingData("2", Collections.singletonList("1"));
        final StitchingEntityData entity3Data = stitchingData("3", Collections.emptyList());
        final StitchingEntityData entity4Data = stitchingData("4", Collections.singletonList("3"));

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1Data,
            "2", entity2Data,
            "3", entity3Data,
            "4", entity4Data
        );

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        final TopologyStitchingEntity entity1 = graph.getEntity(entity1Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity2 = graph.getEntity(entity2Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity3 = graph.getEntity(entity3Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity4 = graph.getEntity(entity4Data.getEntityDtoBuilder()).get();

        assertThat(entity4.getProviders(), contains(entity3));
        assertThat(entity3.getConsumers(), contains(entity4));
        assertThat(entity2.getProviders(), contains(entity1));
        assertThat(entity1.getConsumers(), contains(entity2));
        when(stitchingContext.hasEntity(entity1)).thenReturn(true);

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, entity3, entity1,
            new CommoditySoldMerger(MergeEntities.KEEP_DISTINCT_FAVOR_ONTO));
        when(stitchingContext.hasEntity(entity3)).thenReturn(true);
        merge.applyChange();
        verify(stitchingContext, times(1)).removeEntity(entity3);

        when(stitchingContext.hasEntity(entity3)).thenReturn(false);
        merge.applyChange();
    }

    @Test
    public void testMergeEntityOntoItselfIsNoOp() {
        when(stitchingContext.hasEntity(entity1)).thenReturn(true);

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, entity1, entity1,
            new CommoditySoldMerger(MergeEntities.KEEP_DISTINCT_FAVOR_ONTO));
        merge.applyChange();

        verify(stitchingContext, never()).removeEntity(entity1);
    }

    @Test
    public void testMergeEntityMergesCommoditiesSold() {
        when(stitchingContext.hasEntity(entity2)).thenReturn(true);
        when(stitchingContext.hasEntity(entity3)).thenReturn(true);

        final CommoditySoldMerger merger = mock(CommoditySoldMerger.class);
        when(merger.mergeCommoditiesSold(eq(Collections.emptyList()), eq(Collections.emptyList())))
            .thenReturn(Arrays.asList(
                new CommoditySold(coolingDegC().capacity(10.0).build().toBuilder(), null),
                new CommoditySold(powerWatts().capacity(15.0).build().toBuilder(), null)
            ));

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, entity2, entity3, merger);
        merge.applyChange();

        assertThat(entity3.getCommoditiesSold()
            .map(Builder::getCommodityType)
            .collect(Collectors.toList()), containsInAnyOrder(CommodityType.COOLING, CommodityType.POWER));
    }

    @Test
    public void testMergeCommoditiesBoughtWhenAlreadyBuyingFromProvider() {
        /**
         *   3          3
         *  / \   -->   |
         * 1   2        1
         */
        final StitchingEntityData entity1Data = stitchingData("1", Collections.emptyList());
        final StitchingEntityData entity2Data = stitchingData("2", Collections.emptyList());
        final StitchingEntityData entity3Data = stitchingData("3", EntityType.VIRTUAL_MACHINE,
            buying(CommodityType.CPU, "1"), buying(CommodityType.MEM, "2"));

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1Data,
            "2", entity2Data,
            "3", entity3Data
        );

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        final TopologyStitchingEntity entity1 = graph.getEntity(entity1Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity2 = graph.getEntity(entity2Data.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity entity3 = graph.getEntity(entity3Data.getEntityDtoBuilder()).get();

        when(stitchingContext.hasEntity(entity1)).thenReturn(true);
        when(stitchingContext.hasEntity(entity2)).thenReturn(true);

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, entity2, entity1,
            new CommoditySoldMerger(MergeEntities.KEEP_DISTINCT_FAVOR_ONTO));
        merge.applyChange();

        assertThat(entity3.getCommoditiesBoughtByProvider().get(entity1).stream()
            .map(Builder::getCommodityType)
            .collect(Collectors.toList()), containsInAnyOrder(CommodityType.CPU, CommodityType.MEM));
    }
}