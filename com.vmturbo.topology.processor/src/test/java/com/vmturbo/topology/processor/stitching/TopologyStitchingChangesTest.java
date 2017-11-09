package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.platform.common.builders.CommodityBuilders.cpuMHz;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.newStitchingGraph;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.RemoveEntityChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityAloneChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityRelationshipsChange;

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
            entity5.putProviderCommodities(entity3, Collections.singletonList(cpuMHz().build())))
            .applyChange();
        assertThat(entity5.getProviders(), containsInAnyOrder(entity2, entity3));
        assertThat(entity3.getConsumers(), contains(entity5));
    }
}