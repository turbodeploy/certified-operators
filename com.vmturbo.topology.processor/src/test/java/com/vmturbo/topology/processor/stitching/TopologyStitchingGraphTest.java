package com.vmturbo.topology.processor.stitching;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingOperationResult.RemoveEntityChange;
import com.vmturbo.stitching.StitchingOperationResult.CommoditiesBoughtChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph.UnknownEntityException;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph.Vertex;

public class TopologyStitchingGraphTest {

    /**
     * 4   5
     *  \ /
     *   2   3
     *    \ /
     *     1
     */
    private final StitchingEntityData entity1 = stitchingData("1", Collections.emptyList());
    private final StitchingEntityData entity2 = stitchingData("2", Collections.singletonList("1"));
    private final StitchingEntityData entity3 = stitchingData("3", Collections.singletonList("1"));
    private final StitchingEntityData entity4 = stitchingData("4", Collections.singletonList("2"));
    private final StitchingEntityData entity5 = stitchingData("5", Collections.singletonList("2"));

    private final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
        "1", entity1,
        "2", entity2,
        "3", entity3,
        "4", entity4,
        "5", entity5
    );

    static {
        IdentityGenerator.initPrefix(0);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBuildConstructionEmptyMap() {
        final TopologyStitchingGraph graph = new TopologyStitchingGraph(50);
        assertEquals(0, graph.vertexCount());
        assertEquals(0, producerCount(graph));
        assertEquals(0, consumerCount(graph));
    }

    @Test
    public void testConstruction() {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertEquals(5, graph.vertexCount());
        assertEquals(4, producerCount(graph));
        assertEquals(4, consumerCount(graph));
    }

    @Test
    public void testEntityAppearsMoreThanOnceForSameTarget() {
        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1,
            "2", entity1, // Repeat entity1 and associate it with the wrong OID
            "3", entity3,
            "4", entity4,
            "5", entity5
        );

        expectedException.expect(IllegalArgumentException.class);
        newStitchingGraph(topologyMap);
    }

    @Test
    public void testEntitiesInReverseOrder() {
        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "5", entity5,
            "4", entity4,
            "3", entity3,
            "2", entity2,
            "1", entity1
        );

        // Verify that scanning an entity that links to an entity not yet seen
        // does not result in an error.
        newStitchingGraph(topologyMap);
    }

    @Test
    public void testGetEmptyProducers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(graph.getProviders("1").collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testGetNonEmptyProducers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            graph.getProviders("2")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("1")
        );
    }

    @Test
    public void testMultipleProducers() throws Exception {
        /**
         *    3
         *   / \
         *  1   2
         */
        final StitchingEntityData entity1 = stitchingData("1", Collections.emptyList());
        final StitchingEntityData entity2 = stitchingData("2", Collections.emptyList());
        final StitchingEntityData entity3 = stitchingData("3", Arrays.asList("1", "2"));

        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1,
            "2", entity2,
            "3", entity3
        );

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            graph.getProviders("3")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("1", "2")
        );
    }

    @Test
    public void testGetEmptyConsumers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(graph.getConsumers("5").collect(Collectors.toList()), is(empty()));
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            graph.getConsumers("1")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("2", "3")
        );
        assertThat(
            graph.getConsumers("2")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("4", "5")
        );
    }

    @Test
    public void testGetConsumersNotInGraph() {
        final TopologyStitchingGraph graph = newStitchingGraph(Collections.emptyMap());

        assertThat(
            graph.getConsumers("1").collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetProducersNotInGraph() {
        final TopologyStitchingGraph graph = newStitchingGraph(Collections.emptyMap());

        assertThat(
            graph.getProviders("1").collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testGetVertex() {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertEquals("1", graph.getVertex("1").get().getLocalId());
    }

    @Test
    public void testGetVertexNotInGraph() {
        final TopologyStitchingGraph graph = newStitchingGraph(Collections.emptyMap());

        assertFalse(graph.getVertex("1").isPresent());
    }

    @Test
    public void testProducersForVertexWithMultipleConsumers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            graph.getProviders("2")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("1")
        );
        assertThat(
            graph.getProviders("3")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("1")
        );
    }

    @Test
    public void testConsumersForVertexWithMultipleProducers() throws Exception {
        /**
         *    3
         *   / \
         *  1   2
         */
        final StitchingEntityData entity1 = stitchingData("1", Collections.emptyList());
        final StitchingEntityData entity2 = stitchingData("2", Collections.emptyList());
        final StitchingEntityData entity3 = stitchingData("3", Arrays.asList("1", "2"));
        
        final Map<String, StitchingEntityData> topologyMap = ImmutableMap.of(
            "1", entity1,
            "2", entity2,
            "3", entity3
        );

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            graph.getConsumers("1")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("3")
        );
        assertThat(
            graph.getConsumers("2")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("3")
        );
    }

    @Test
    public void testMultipleEntitiesSameOid() {
        final StitchingEntityData e1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e2 = stitchingData("1", Collections.emptyList()).forTarget(2L);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1, topologyMapOf(e1));
        graph.addStitchingData(e2, topologyMapOf(e2));

        assertEquals(1, graph.vertexCount());
        assertEquals(e1.getEntityDtoBuilder(), graph.getEntityBuilder("1", 1L).get());
        assertEquals(e2.getEntityDtoBuilder(), graph.getEntityBuilder("1", 2L).get());
    }

    @Test
    public void testMultipleEntitySameOidProvidersConsumers() {
        /**
         *   3|3 (entities from multiple targets are buying from different providers. Vertex should buy from union.)
         *   / \
         *  1   2
         */
        final StitchingEntityData e1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e3_1 = stitchingData("3", Collections.singletonList("1")).forTarget(2L);
        final Map<String, StitchingEntityData> target1Graph = topologyMapOf(e1, e3_1);

        final StitchingEntityData e2 = stitchingData("2", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e3_2 = stitchingData("3", Collections.singletonList("2")).forTarget(2L);
        final Map<String, StitchingEntityData> target2Graph = topologyMapOf(e2, e3_2);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1, target1Graph);
        graph.addStitchingData(e3_1, target1Graph);
        graph.addStitchingData(e2, target2Graph);
        graph.addStitchingData(e3_2, target2Graph);

        assertEquals(3, graph.vertexCount());
        assertThat(graph.getProviders("3")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("1", "2"));

        assertThat(graph.getConsumers("1")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("3"));
        assertThat(graph.getConsumers("2")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("3"));
    }

    @Test
    public void testRemoveNotExisting() {
        // Removing something not in the graph is treated as a no-op
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        final Set<String> updatedByRemoval = graph.removeEntity(new RemoveEntityChange(
            EntityDTO.newBuilder()
                .setId("-1")
                .setEntityType(EntityType.VIRTUAL_MACHINE)
        ));

        assertThat(updatedByRemoval, is(empty()));
    }

    @Test
    public void testRemoveAffectsConsumerRelationships() {
        /**
         * 4   5            4   5
         *  \ /              \ /
         *   2   3   -->      2    3
         *    \ /
         *     1
         */
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        // Before removing, 2 and 3 should be buying from 1.
        assertThat(graph.getProviders("2")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("1"));
        assertThat(graph.getProviders("3")
                .map(Vertex::getLocalId)
                .collect(Collectors.toList()),
            contains("1"));

        final Set<String> updatedByRemoval = graph.removeEntity(new RemoveEntityChange(entity1.getEntityDtoBuilder()));

        assertFalse(graph.getVertex("1").isPresent());
        assertThat(updatedByRemoval, containsInAnyOrder("1", "2", "3")); // 2 and 3 had their producers relations updated.
        assertThat(graph.getProviders("2")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()),
            is(empty()));
        assertThat(graph.getProviders("3")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testRemoveAffectsConsumerCommodities() {
        /**
         * 4   5            4   5
         *  \ /              \ /
         *   2   3   -->      2    3
         *    \ /
         *     1
         */
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        // Before removing, 2 and 3 should be buying from 1.
        assertThat(entity2.getEntityDtoBuilder(), isBuyingCommodityFrom("1"));
        assertThat(entity3.getEntityDtoBuilder(), isBuyingCommodityFrom("1"));

        graph.removeEntity(new RemoveEntityChange(entity1.getEntityDtoBuilder()));

        assertThat(entity2.getEntityDtoBuilder(), not(isBuyingCommodityFrom("1")));
        assertThat(entity3.getEntityDtoBuilder(), not(isBuyingCommodityFrom("1")));
    }

    @Test
    public void testRemoveEntityWithOtherHavingSameOid() {
        final StitchingEntityData e1_1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e1_2 = stitchingData("1", Collections.emptyList()).forTarget(2L);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1_1, topologyMapOf(e1_1));
        graph.addStitchingData(e1_2, topologyMapOf(e1_2));

        assertEquals(1, graph.vertexCount());
        graph.removeEntity(new RemoveEntityChange(e1_1.getEntityDtoBuilder()));
        assertEquals(1, graph.vertexCount());
        graph.removeEntity(new RemoveEntityChange(e1_2.getEntityDtoBuilder()));
        assertEquals(0, graph.vertexCount());
    }

    @Test
    public void testRemoveWhenMultipleProvidersFromDifferentTargetsSameOid() {
        /**
         *    2
         *    |
         *   1|1   2 is buying from 1, but from an entity with OID 1 that was discovered by a different
         *         target than discovered 2.
         */
        final StitchingEntityData e1_1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e1_2 = stitchingData("1", Collections.emptyList()).forTarget(2L);
        final StitchingEntityData e2_2 = stitchingData("2", Collections.singletonList("1")).forTarget(2L);
        final Map<String, StitchingEntityData> target1Graph = topologyMapOf(e1_1);
        final Map<String, StitchingEntityData> target2Graph = topologyMapOf(e1_2, e2_2);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1_1, target1Graph);
        graph.addStitchingData(e1_2, target2Graph);
        graph.addStitchingData(e2_2, target2Graph);

        // e2_2 should buy from e1_2 both before and after removing e1_1 because it was discovered from a different
        // target than e1_1.
        assertThat(e2_2.getEntityDtoBuilder(), isBuyingCommodityFrom("1"));
        graph.removeEntity(new RemoveEntityChange(e1_1.getEntityDtoBuilder()));
        assertThat(e2_2.getEntityDtoBuilder(), isBuyingCommodityFrom("1"));
    }

    @Test
    public void testUpdateNotExisting() {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        expectedException.expect(UnknownEntityException.class);
        graph.updateCommoditiesBought(new CommoditiesBoughtChange(EntityDTO.newBuilder()
            .setId("-1")
            .setEntityType(EntityType.VIRTUAL_MACHINE), entity -> {
        }));
    }

    @Test
    public void testRemoveBoughtRemovesSellers() {
        /**
         * 4   5            4   5
         *  \ /              \ /
         *   2   3   -->      2   3
         *    \ /                /
         *     1                1
         */
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        final Set<String> modifiedEntities = graph.updateCommoditiesBought(
            new CommoditiesBoughtChange(entity2.getEntityDtoBuilder(), Builder::clearCommoditiesBought));
        final List<String> consumersOf1 = graph.getConsumers("1")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList());

        assertThat(modifiedEntities, containsInAnyOrder("2", "1"));
        assertThat(graph.getProviders("2").collect(Collectors.toList()), is(empty()));
        assertThat(consumersOf1, not(contains("2")));
        assertThat(consumersOf1, contains("3"));
    }

    @Test
    public void testAddBoughtAddsSellers() {
        /**
         * 4   5            4   5
         *  \ /              \ / \
         *   2   3   -->      2   3
         *    \ /              \ /
         *     1                1
         */
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(graph.getProviders("5")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()), contains("2"));
        assertThat(graph.getConsumers("3")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()), is(empty()));

        final Set<String> modifiedEntities = graph.updateCommoditiesBought(
            new CommoditiesBoughtChange(entity5.getEntityDtoBuilder(), entity ->
                entity.addCommoditiesBought(CommodityBought.newBuilder().setProviderId("3"))));
        assertThat(modifiedEntities, containsInAnyOrder("5", "3"));

        assertThat(graph.getProviders("5")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()), containsInAnyOrder("2", "3"));
        assertThat(graph.getConsumers("3")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()), contains("5"));
    }

    @Test
    public void testRemoveBoughtWhenOtherEntityStillBuying() {
        /**
         * 2_1|2_2  // Both 2_1 and 2_2 are buying from 1. Remove the relationship from 2_1,
         *    |     // because 2_2 is still buying from 1, the 2 vertex should still be
         * 1_1|1_2  // reachable from 1.
         */
        final StitchingEntityData e1_1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e1_2 = stitchingData("1", Collections.emptyList()).forTarget(2L);
        final StitchingEntityData e2_1 = stitchingData("2", Collections.singletonList("1")).forTarget(1L);
        final StitchingEntityData e2_2 = stitchingData("2", Collections.singletonList("1")).forTarget(2L);
        final Map<String, StitchingEntityData> target1Graph = topologyMapOf(e1_1, e2_1);
        final Map<String, StitchingEntityData> target2Graph = topologyMapOf(e1_2, e2_2);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1_1, target1Graph);
        graph.addStitchingData(e1_2, target2Graph);
        graph.addStitchingData(e2_1, target2Graph);
        graph.addStitchingData(e2_2, target2Graph);

        graph.updateCommoditiesBought(new CommoditiesBoughtChange(e2_1.getEntityDtoBuilder(),
            e -> e.removeCommoditiesBought(0)));

        // First removal should leave relationship in place because e2_2 is still buying from e1_2
        assertThat(graph.getProviders("2")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()), contains("1"));

        graph.updateCommoditiesBought(new CommoditiesBoughtChange(e2_2.getEntityDtoBuilder(),
            e -> e.removeCommoditiesBought(0)));

        // Second removal should clear the relationship entirely
        assertThat(graph.getProviders("2")
            .map(Vertex::getLocalId)
            .collect(Collectors.toList()), is(empty()));
    }

    @Nonnull StitchingDataAllowingTargetChange stitchingData(@Nonnull final String localId,
                                                             @Nonnull final List<String> providerIds) {
        final EntityDTO.Builder builder = EntityDTO.newBuilder()
            .setId(localId)
            .setEntityType(EntityType.VIRTUAL_MACHINE);

        for (String providerId : providerIds) {
            builder.addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId(providerId));
        }

        return stitchingData(builder);
    }

    @Nonnull
    private StitchingDataAllowingTargetChange stitchingData(@Nonnull final EntityDTO.Builder builder) {
        final long DEFAULT_TARGET_ID = 12345L;

        return new StitchingDataAllowingTargetChange(builder, DEFAULT_TARGET_ID, IdentityGenerator.next());
    }

    @Nonnull
    private Map<String, StitchingEntityData> topologyMapOf(@Nonnull final StitchingEntityData... entities) {
        final Map<String, StitchingEntityData> map = new HashMap<>(entities.length);
        for (StitchingEntityData entity : entities) {
            map.put(entity.getEntityDtoBuilder().getId(), entity);
        }

        return map;
    }

    @Nonnull
    private TopologyStitchingGraph newStitchingGraph(@Nonnull final Map<String, StitchingEntityData> topologyMap) {
        final TopologyStitchingGraph graph = new TopologyStitchingGraph(topologyMap.size());
        topologyMap.values().forEach(entity -> graph.addStitchingData(entity, topologyMap));

        return graph;
    }

    private int producerCount(@Nonnull final TopologyStitchingGraph graph) {
        return graph.vertices().mapToInt(
            vertex -> graph.getProviders(vertex)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }

    private int consumerCount(@Nonnull final TopologyStitchingGraph graph) {
        return graph.vertices().mapToInt(
            vertex -> graph.getConsumers(vertex)
                .collect(Collectors.summingInt(v -> 1))
        ).sum();
    }

    private static class StitchingDataAllowingTargetChange extends StitchingEntityData {
        public StitchingDataAllowingTargetChange(@Nonnull final EntityDTO.Builder entityDtoBuilder,
                                                 final long targetId,
                                                 final long oid) {
            super(entityDtoBuilder, targetId, oid);
        }

        public StitchingEntityData forTarget(final long targetId) {
            return new StitchingEntityData(getEntityDtoBuilder(), targetId, IdentityGenerator.next());
        }
    }

    /**
     * A matcher that allows asserting that a particular entity in the topology is acting as a provider
     * for exactly a certain number of entities.
     */
    public static Matcher<EntityDTO.Builder> isBuyingCommodityFrom(final String providerOid) {
        return new BaseMatcher<EntityDTO.Builder>() {
            @Override
            @SuppressWarnings("unchecked")
            public boolean matches(Object o) {
                final EntityDTO.Builder entity = (EntityDTO.Builder) o;
                for (CommodityBought bought : entity.getCommoditiesBoughtList()) {
                    if (providerOid.equals(bought.getProviderId())) {
                        return true;
                    }
                }

                return false;
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("Entity should be buying a commodity from provider with oid " +
                    providerOid);
            }
        };
    }
}