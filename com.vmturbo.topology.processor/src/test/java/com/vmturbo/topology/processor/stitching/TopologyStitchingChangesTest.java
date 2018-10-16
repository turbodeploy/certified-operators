package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.coolingDegC;
import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.platform.common.builders.CommodityBuilders.powerWatts;
import static com.vmturbo.stitching.utilities.MergeEntities.*;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.buying;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.newStitchingGraph;
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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.EntityFieldMergers.EntityFieldMerger;
import com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.MergeEntitiesChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.RemoveEntityChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityAloneChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityRelationshipsChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;

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

    final StitchingJournal journal = new StitchingJournal<>();

    static {
        IdentityGenerator.initPrefix(0);
    }

    @Test
    public void testRemoveEntityChange() {
        final RemoveEntityChange change = new RemoveEntityChange(stitchingContext, entity1);
        change.applyChange(new StitchingJournal<>());

        verify(stitchingContext).removeEntity(entity1);
    }

    @Test
    public void testRemoveEntityMultipleTimesDoesNotThrowException() {
        final RemoveEntityChange change = new RemoveEntityChange(stitchingContext, entity1);
        change.applyChange(new StitchingJournal<>());
        change.applyChange(new StitchingJournal<>());
    }

    @Test
    public void testUpdateAloneChange() {
        final UpdateEntityAloneChange change = new UpdateEntityAloneChange<>(entity2,
            e -> e.getEntityBuilder().setDisplayName("foo"));

        assertNotEquals("foo", entity2.getDisplayName());
        change.applyChange(new StitchingJournal<>());
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
            .applyChange(new StitchingJournal<>());
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
            entity5.addProviderCommodityBought(entity3,
                    new CommoditiesBought(Collections.singletonList(cpuMHz().build().toBuilder()))))
            .applyChange(new StitchingJournal<>());
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
        final StitchingEntityData entity1Data = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData entity2Data = stitchingData("2", Collections.singletonList("1")).forTarget(1L);
        final StitchingEntityData entity3Data = stitchingData("3", Collections.emptyList()).forTarget(2L);
        final StitchingEntityData entity4Data = stitchingData("4", Collections.singletonList("3")).forTarget(2L);

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
            new CommoditySoldMerger(KEEP_DISTINCT_FAVOR_ONTO),
            Collections.emptyList()).applyChange(new StitchingJournal<>());

        verify(stitchingContext).removeEntity(entity3);
        assertThat(entity4.getProviders(), contains(entity1));
        assertThat(entity2.getProviders(), contains(entity1));
        assertThat(entity1.getConsumers(), containsInAnyOrder(entity2, entity4));

        assertEquals(1L, entity1.getTargetId());
        assertThat(entity1.getMergeInformation(), contains(new StitchingMergeInformation(entity3)));
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
            new CommoditySoldMerger(KEEP_DISTINCT_FAVOR_ONTO), Collections.emptyList());
        when(stitchingContext.hasEntity(entity3)).thenReturn(true);
        merge.applyChange(new StitchingJournal<>());
        verify(stitchingContext, times(1)).removeEntity(entity3);

        when(stitchingContext.hasEntity(entity3)).thenReturn(false);
        merge.applyChange(new StitchingJournal<>());
    }

    @Test
    public void testMergeEntitiesCarriesOverMergeTargets() {
        final StitchingEntityData entity1Data = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData entity2Data = stitchingData("2", Collections.emptyList()).forTarget(2L);
        final StitchingEntityData entity3Data = stitchingData("3", Collections.emptyList()).forTarget(3L);

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1Data,
            "2", entity2Data,
            "3", entity3Data
        );
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        final TopologyStitchingEntity entity1 = graph.getEntity(entity1Data.getEntityDtoBuilder()).get();
        entity1.updateLastUpdatedTime(1000L);
        final TopologyStitchingEntity entity2 = graph.getEntity(entity2Data.getEntityDtoBuilder()).get();
        entity2.updateLastUpdatedTime(2000L);
        final TopologyStitchingEntity entity3 = graph.getEntity(entity3Data.getEntityDtoBuilder()).get();
        entity3.updateLastUpdatedTime(3000L);
        when(stitchingContext.hasEntity(entity1)).thenReturn(true);
        when(stitchingContext.hasEntity(entity2)).thenReturn(true);
        when(stitchingContext.hasEntity(entity3)).thenReturn(true);

        final MergeEntitiesChange mergeThreeOntoTwo = new MergeEntitiesChange(stitchingContext, entity3, entity2,
            new CommoditySoldMerger(KEEP_DISTINCT_FAVOR_ONTO), Collections.emptyList());
        final MergeEntitiesChange mergeTwoOntoOne = new MergeEntitiesChange(stitchingContext, entity2, entity1,
            new CommoditySoldMerger(KEEP_DISTINCT_FAVOR_ONTO), Collections.emptyList());
        assertEquals(1000L, entity1.getLastUpdatedTime());
        assertEquals(2000L, entity2.getLastUpdatedTime());

        mergeThreeOntoTwo.applyChange(journal);
        assertThat(entity2.getMergeInformation(), contains(new StitchingMergeInformation(entity3)));
        assertEquals(3000L, entity2.getLastUpdatedTime());

        mergeTwoOntoOne.applyChange(journal);
        assertThat(entity1.getMergeInformation(),
            containsInAnyOrder(new StitchingMergeInformation(entity3), new StitchingMergeInformation(entity2)));
        assertEquals(3000L, entity1.getLastUpdatedTime());
    }

    @Test
    public void testMergeEntityOntoItselfIsNoOp() {
        when(stitchingContext.hasEntity(entity1)).thenReturn(true);

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, entity1, entity1,
            new CommoditySoldMerger(KEEP_DISTINCT_FAVOR_ONTO), Collections.emptyList());
        merge.applyChange(new StitchingJournal<>());

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

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, entity2, entity3,
            merger, Collections.emptyList());
        merge.applyChange(new StitchingJournal<>());

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
            new CommoditySoldMerger(KEEP_DISTINCT_FAVOR_ONTO), Collections.emptyList());
        merge.applyChange(new StitchingJournal<>());

        assertThat(entity3.getCommodityBoughtListByProvider().get(entity1).stream()
                .flatMap(commodityBought -> commodityBought.getBoughtList().stream())
                .map(Builder::getCommodityType)
                .collect(Collectors.toList()), containsInAnyOrder(CommodityType.CPU, CommodityType.MEM));
    }

    @Test
    public void testMergeEntitiesMergesFields() {
        /**
         * foo / bar  -->  baz
         */
        final StitchingEntityData foo = stitchingData("1", Collections.emptyList());
        final StitchingEntityData bar = stitchingData("2", Collections.emptyList());

        foo.getEntityDtoBuilder().setDisplayName("foo");
        bar.getEntityDtoBuilder().setDisplayName("bar");

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", foo,
            "2", bar);

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        final TopologyStitchingEntity from = graph.getEntity(foo.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity onto = graph.getEntity(bar.getEntityDtoBuilder()).get();

        when(stitchingContext.hasEntity(from)).thenReturn(true);
        when(stitchingContext.hasEntity(onto)).thenReturn(true);

        final MergeEntitiesDetails mergeDetails =
            mergeEntity(from)
            .onto(onto)
            .addFieldMerger(EntityFieldMergers
                .merge(EntityDTOOrBuilder::getDisplayName, EntityDTO.Builder::setDisplayName)
                .withMethod((a, b) -> "baz"));

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, mergeDetails);
        merge.applyChange(new StitchingJournal<>());

        assertEquals("baz", onto.getDisplayName());
    }

    @Test
    public void testMergeEntitiesFieldOrder() {
        /**
         * foo / bar  -->  quux
         */
        final StitchingEntityData foo = stitchingData("1", Collections.emptyList());
        final StitchingEntityData bar = stitchingData("2", Collections.emptyList());

        foo.getEntityDtoBuilder().setDisplayName("foo");
        bar.getEntityDtoBuilder().setDisplayName("bar");

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", foo,
            "2", bar);

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        final TopologyStitchingEntity from = graph.getEntity(foo.getEntityDtoBuilder()).get();
        final TopologyStitchingEntity onto = graph.getEntity(bar.getEntityDtoBuilder()).get();

        when(stitchingContext.hasEntity(from)).thenReturn(true);
        when(stitchingContext.hasEntity(onto)).thenReturn(true);

        final EntityFieldMerger<String> mergeToBaz = EntityFieldMergers
            .merge(EntityDTOOrBuilder::getDisplayName, EntityDTO.Builder::setDisplayName)
            .withMethod((a, b) -> "baz");
        final EntityFieldMerger<String> mergeToQuux = EntityFieldMergers
            .merge(EntityDTOOrBuilder::getDisplayName, EntityDTO.Builder::setDisplayName)
            .withMethod((a, b) -> "quux");

        final MergeEntitiesDetails mergeDetails = mergeEntity(from)
            .onto(onto)
            .addFieldMerger(mergeToBaz)   // First overrides to baz
            .addFieldMerger(mergeToQuux); // Then overrides again to quux

        final MergeEntitiesChange merge = new MergeEntitiesChange(stitchingContext, mergeDetails);
        merge.applyChange(new StitchingJournal<>());

        assertEquals("quux", onto.getDisplayName());

    }
}