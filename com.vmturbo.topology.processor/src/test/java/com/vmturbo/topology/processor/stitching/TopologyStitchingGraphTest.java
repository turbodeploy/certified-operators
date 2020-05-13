package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.isBuyingCommodityFrom;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.newStitchingGraph;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors.StitchingErrorCode;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ActionOnProviderEligibility;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SubDivisionData;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;

public class TopologyStitchingGraphTest {

    private final StitchingEntityData validEntity = stitchingData(EntityDTO.newBuilder()
        .setId("valid-provider")
        .setDisplayName("valid-provider")
        .setEntityType(EntityType.VIRTUAL_MACHINE));

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
    public void testInvalidCommBought() {
        final CommodityBought validCommBought = CommodityBought.newBuilder()
            .setProviderId(validEntity.getLocalId())
            .addBought(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.MEM))
            .build();

        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId("bad provider"))
            .addCommoditiesBought(validCommBought)
            .addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId("another bad provider")));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertTrue(resEntity.getStitchingErrors().contains(StitchingErrorCode.INVALID_COMM_BOUGHT));
        assertThat(resEntity.getEntityBuilder().getCommoditiesBoughtList(),
            containsInAnyOrder(validCommBought));
    }

    @Test
    public void testInvalidCommBoughtSubdivision() {
        final CommodityBought validCommBought = CommodityBought.newBuilder()
            .setProviderId(validEntity.getLocalId())
            .addBought(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.MEM))
            .build();

        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId(validEntity.getLocalId())
                .setSubDivision(SubDivisionData.newBuilder()
                    .setSubDivisionId("bad subdivision")))
            .addCommoditiesBought(validCommBought)
            .addCommoditiesBought(CommodityBought.newBuilder()
                .setProviderId(validEntity.getLocalId())
                .setSubDivision(SubDivisionData.newBuilder()
                    .setSubDivisionId("another bad subdivision"))));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertTrue(resEntity.getStitchingErrors().contains(StitchingErrorCode.INVALID_COMM_BOUGHT));
        assertThat(resEntity.getEntityBuilder().getCommoditiesBoughtList(),
            containsInAnyOrder(validCommBought));
    }

    @Test
    public void testInvalidCommSold() {
        final CommodityDTO validCommSold = CommodityDTO.newBuilder()
            .setCommodityType(CommodityType.MEM)
            .setUsed(10)
            .build();

        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.DSPM_ACCESS)
                .setKey("PhysicalMachine::bad provider"))
            .addCommoditiesSold(validCommSold)
            .addCommoditiesSold(CommodityDTO.newBuilder()
                .setCommodityType(CommodityType.DSPM_ACCESS)
                .setKey("PhysicalMachine::another bad provider")));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertTrue(resEntity.getStitchingErrors().contains(StitchingErrorCode.INVALID_COMM_SOLD));
        assertThat(resEntity.getEntityBuilder().getCommoditiesSoldList(),
            containsInAnyOrder(validCommSold));
    }

    @Test
    public void testInvalidLayeredOver() {
        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addLayeredOver("bad layered over")
            .addLayeredOver(validEntity.getLocalId())
            .addLayeredOver("another bad layered over"));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertTrue(resEntity.getStitchingErrors().contains(StitchingErrorCode.INVALID_LAYERED_OVER));
        assertThat(resEntity.getEntityBuilder().getLayeredOverList(),
            containsInAnyOrder(validEntity.getLocalId()));
    }

    @Test
    public void testDuplicateLayeredOver() {
        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addLayeredOver(validEntity.getLocalId())
            .addLayeredOver(validEntity.getLocalId())
            .addLayeredOver(validEntity.getLocalId()));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertThat(resEntity.getEntityBuilder().getLayeredOverList(),
            containsInAnyOrder(validEntity.getLocalId()));
    }

    @Test
    public void testInvalidConsistsOf() {
        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addConsistsOf("bad consists of")
            .addConsistsOf(validEntity.getLocalId())
            .addConsistsOf("another bad consists of"));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertTrue(resEntity.getStitchingErrors().contains(StitchingErrorCode.INVALID_CONSISTS_OF));
        assertThat(resEntity.getEntityBuilder().getConsistsOfList(),
            containsInAnyOrder(validEntity.getLocalId()));
    }

    @Test
    public void testDuplicateConsistsOf() {
        final StitchingEntityData entity = stitchingData(entityBuilder()
            .addConsistsOf(validEntity.getLocalId())
            .addConsistsOf(validEntity.getLocalId())
            .addConsistsOf(validEntity.getLocalId()));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        assertThat(resEntity.getEntityBuilder().getConsistsOfList(),
            containsInAnyOrder(validEntity.getLocalId()));
    }


    @Test
    public void testBuildConstructionEmptyMap() {
        final TopologyStitchingGraph graph = new TopologyStitchingGraph(50);
        assertEquals(0, graph.entityCount());
        assertEquals(0, producerCount(graph));
        assertEquals(0, consumerCount(graph));
    }

    @Test
    public void testConstruction() {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertEquals(5, graph.entityCount());
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

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        assertEquals(4, graph.entityCount());
        assertTrue(graph.getEntity(entity4.getEntityDtoBuilder()).get().getStitchingErrors()
            .contains(StitchingErrorCode.INVALID_COMM_BOUGHT, StitchingErrorCode.INCONSISTENT_KEY));
    }

    @Test
    public void testEntityBuyingFromEntityNotPresent() {
        // Entity 1 buys from entity 2, but entity 2 is not present.
        final StitchingEntityData entity = stitchingData("1", Collections.singletonList("2"));

        final Map<String,StitchingEntityData> topologyMap = ImmutableMap.of("1", entity);

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);
        assertTrue(graph.getEntity(entity.getEntityDtoBuilder()).get().getStitchingErrors()
            .contains(StitchingErrorCode.INVALID_COMM_BOUGHT));

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

        assertThat(providersFor(graph, "1"), is(empty()));
    }

    @Test
    public void testGetNonEmptyProducers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            providersFor(graph, "2").stream()
                .map(StitchingEntity::getLocalId)
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
            providersFor(graph, "3").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("1", "2")
        );
    }

    @Test
    public void testGetEmptyConsumers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(consumersFor(graph, "5"), is(empty()));
    }

    @Test
    public void testMultipleConsumers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            consumersFor(graph, "1").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("2", "3")
        );
        assertThat(
            consumersFor(graph, "2").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("4", "5")
        );
    }

    @Test
    public void testGetVertex() {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertEquals("1", entityByLocalId(graph, "1").get().getLocalId());
    }

    @Test
    public void testGetVertexNotInGraph() {
        final TopologyStitchingGraph graph = newStitchingGraph(Collections.emptyMap());

        assertFalse(entityByLocalId(graph, "1").isPresent());
    }

    @Test
    public void testProducersForVertexWithMultipleConsumers() throws Exception {
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        assertThat(
            providersFor(graph, "2").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            containsInAnyOrder("1")
        );
        assertThat(
            providersFor(graph, "3").stream()
                .map(StitchingEntity::getLocalId)
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
            consumersFor(graph, "1").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            contains("3")
        );
        assertThat(
            consumersFor(graph, "2").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            contains("3")
        );
    }

    @Test
    public void testKeyToUuid() {
        final String onpremKey = "PhysicalMachine::7cd62bff-d6c8-e011-0000-00000000000f";
        final String cloudKey = "PhysicalMachine::aws::us-west-2::PM::us-west-2b";
        final String onpremUuid = "7cd62bff-d6c8-e011-0000-00000000000f";
        final String cloudUuid = "aws::us-west-2::PM::us-west-2b";
        assertEquals(onpremUuid, SdkToTopologyEntityConverter.keyToUuid(onpremKey));
        assertEquals(cloudUuid, SdkToTopologyEntityConverter.keyToUuid(cloudKey));
    }

    @Test
    public void testMultipleEntitiesSameOid() {
        final StitchingEntityData e1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e2 = stitchingData("1", Collections.emptyList()).forTarget(2L);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1, topologyMapOf(e1));
        graph.addStitchingData(e2, topologyMapOf(e2));

        assertEquals(2, graph.entityCount());
        final List<EntityDTO.Builder> entitiesWithId1 = graph.entities()
            .filter(entity -> entity.getLocalId().equals("1"))
            .map(TopologyStitchingEntity::getEntityBuilder)
            .collect(Collectors.toList());

        assertThat(entitiesWithId1, containsInAnyOrder(e1.getEntityDtoBuilder(), e2.getEntityDtoBuilder()));
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

        assertEquals(4, graph.entityCount());
        assertThat(graph.getEntity(e3_1.getEntityDtoBuilder()).get()
                .getProviders().stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            contains("1"));
        assertThat(graph.getEntity(e3_2.getEntityDtoBuilder()).get()
                .getProviders().stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            contains("2"));

        assertThat(consumersFor(graph, "1").stream()
                .map(StitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            contains(e3_1.getEntityDtoBuilder()));
        assertThat(consumersFor(graph, "2").stream()
                .map(StitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            contains(e3_2.getEntityDtoBuilder()));
    }

    @Test
    public void testRemoveNotExisting() {
        // Removing something not in the graph is treated as a no-op
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        final TopologyStitchingEntity toRemove =
            new TopologyStitchingEntity(stitchingData("6", Collections.emptyList()));
        assertTrue(graph.removeEntity(toRemove).isEmpty());
    }

    @Test
    public void testRemoveClearsConsumersAndProviders() {
        // Removing something not in the graph is treated as a no-op
        final TopologyStitchingGraph graph = newStitchingGraph(topologyMap);

        final TopologyStitchingEntity toRemove = graph.getEntity(entity1.getEntityDtoBuilder()).get();
        graph.removeEntity(toRemove);
        assertTrue(toRemove.getConsumers().isEmpty());
        assertTrue(toRemove.getProviders().isEmpty());
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
        assertThat(providersFor(graph, "2").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            contains("1"));
        assertThat(providersFor(graph, "3").stream()
                .map(StitchingEntity::getLocalId)
                .collect(Collectors.toList()),
            contains("1"));

        final TopologyStitchingEntity toRemove = graph.getEntity(entity1.getEntityDtoBuilder()).get();
        final List<String> affectedByRemoval = graph.removeEntity(toRemove).stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList());

        assertFalse(entityByLocalId(graph, "1").isPresent());
        assertThat(affectedByRemoval, containsInAnyOrder("1", "2", "3")); // 2 and 3 had their producers relations updated.
        assertThat(providersFor(graph, "2").stream()
            .map(StitchingEntity::getLocalId)
            .collect(Collectors.toList()),
            is(empty()));
        assertThat(providersFor(graph, "3").stream()
            .map(StitchingEntity::getLocalId)
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
        assertThat(graph.getEntity(entity2.getEntityDtoBuilder()).get(), isBuyingCommodityFrom("1"));
        assertThat(graph.getEntity(entity3.getEntityDtoBuilder()).get(), isBuyingCommodityFrom("1"));

        final TopologyStitchingEntity toRemove = graph.getEntity(entity1.getEntityDtoBuilder()).get();
        graph.removeEntity(toRemove);

        assertThat(graph.getEntity(entity2.getEntityDtoBuilder()).get(), not(isBuyingCommodityFrom("1")));
        assertThat(graph.getEntity(entity3.getEntityDtoBuilder()).get(), not(isBuyingCommodityFrom("1")));
    }

    @Test
    public void testRemoveEntityWithOtherHavingSameOid() {
        final StitchingEntityData e1_1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
        final StitchingEntityData e1_2 = stitchingData("1", Collections.emptyList()).forTarget(2L);

        final TopologyStitchingGraph graph = new TopologyStitchingGraph(5);
        graph.addStitchingData(e1_1, topologyMapOf(e1_1));
        graph.addStitchingData(e1_2, topologyMapOf(e1_2));

        assertEquals(2, graph.entityCount());
        graph.removeEntity(graph.getEntity(e1_1.getEntityDtoBuilder()).get());
        assertEquals(1, graph.entityCount());
        graph.removeEntity(graph.getEntity(e1_2.getEntityDtoBuilder()).get());
        assertEquals(0, graph.entityCount());
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
        assertThat(graph.getEntity(e2_2.getEntityDtoBuilder()).get(), isBuyingCommodityFrom("1"));
        graph.removeEntity(graph.getEntity(e1_1.getEntityDtoBuilder()).get());
        assertThat(graph.getEntity(e2_2.getEntityDtoBuilder()).get(), isBuyingCommodityFrom("1"));
    }

    /**
     * Test that the action eligibility data set in the commodity bought section of
     * the SDK's entity DTO is transferred to the TopologyStitchingEntity.
     */
    @Test
    public void testMoveEligibilityData() {
        final CommodityBought validCommBought = CommodityBought.newBuilder()
                .setProviderId(validEntity.getLocalId())
                .setActionEligibility(ActionOnProviderEligibility.newBuilder()
                        .setMovable(false)
                        .build())
                .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.MEM))
                .build();

        final StitchingEntityData entity = stitchingData(entityBuilder()
                .addCommoditiesBought(validCommBought));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        Map<StitchingEntity, List<CommoditiesBought>> commBoughtList = resEntity.getCommodityBoughtListByProvider();
        for (StitchingEntity se : commBoughtList.keySet()) {
            List<CommoditiesBought> bought =   commBoughtList.get(se);
            for (CommoditiesBought b : bought) {
                assertEquals(b.getMovable().get(), false);
            }
        }
    }

    /**
     * Test that the CommoditiesBought of the TopologyStitchingEntity does not contain any values
     * for the action eligibility attributes if the action eligibility data
     * is not set in the commodity bought section of the SDK's entity DTO.
     */
    @Test
    public void testUnsetMoveActionEligibilityData() {
        final CommodityBought validCommBought = CommodityBought.newBuilder()
                .setProviderId(validEntity.getLocalId())
                .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.MEM))
                .build();

        final StitchingEntityData entity = stitchingData(entityBuilder()
                .addCommoditiesBought(validCommBought));

        final TopologyStitchingGraph graph = newStitchingGraph(topologyMapOf(validEntity, entity));

        assertThat(graph.entityCount(), is(2));
        final TopologyStitchingEntity resEntity = graph.getEntity(entity.getEntityDtoBuilder()).get();
        Map<StitchingEntity, List<CommoditiesBought>> commBoughtList = resEntity.getCommodityBoughtListByProvider();
        for (StitchingEntity se : commBoughtList.keySet()) {
            List<CommoditiesBought> bought =   commBoughtList.get(se);
            for (CommoditiesBought b : bought) {
                assertEquals(b.getMovable(), Optional.empty());
            }
        }
    }

    private Optional<TopologyStitchingEntity> entityByLocalId(@Nonnull final TopologyStitchingGraph graph,
                                                              @Nonnull final String entityLocalId) {
        return graph.entities()
            .filter(entity -> entity.getLocalId().equals(entityLocalId))
            .findFirst();
    }

    /**
     * Get the providers for an entity with a given localId.
     * Throws a null pointer if no entity with the given id is in the graph.
     * If multiple entities with the id are in the graph, returns one of them without making any guarantees
     * about which you get.
     *
     * @param graph The graph.
     * @param entityLocalId The id of the entity whose providers should be retrieved.
     * @return providers for the entity with the given local id
     */
    private Set<StitchingEntity> providersFor(@Nonnull final TopologyStitchingGraph graph,
                                              @Nonnull final String entityLocalId) {
        return entityByLocalId(graph, entityLocalId).get()
            .getProviders();
    }

    /**
     * Get the providers for an entity with a given localId.
     * Throws a null pointer if no entity with the given id is in the graph.
     * If multiple entities with the id are in the graph, returns one of them without making any guarantees
     * about which you get.
     *
     * @param graph The graph.
     * @param entityLocalId The id of the entity whose providers should be retrieved.
     * @return providers for the entity with the given local id
     */
    private Set<StitchingEntity> consumersFor(@Nonnull final TopologyStitchingGraph graph,
                                                 @Nonnull final String entityLocalId) {
        return entityByLocalId(graph, entityLocalId).get()
            .getConsumers();
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

    private int producerCount(@Nonnull final TopologyStitchingGraph graph) {
        return graph.entities().mapToInt(
            entity -> entity.getProviders().size()
        ).sum();
    }

    private int consumerCount(@Nonnull final TopologyStitchingGraph graph) {
        return graph.entities().mapToInt(
            entity -> entity.getConsumers().size()
        ).sum();
    }

    private static class StitchingDataAllowingTargetChange extends StitchingEntityData {
        public StitchingDataAllowingTargetChange(@Nonnull final EntityDTO.Builder entityDtoBuilder,
                                                 final long targetId,
                                                 final long oid) {
            super(entityDtoBuilder, targetId, oid, 0, true);
        }

        public StitchingEntityData forTarget(final long targetId) {
            return StitchingEntityData.newBuilder(getEntityDtoBuilder())
                .targetId(targetId)
                .oid(IdentityGenerator.next())
                .lastUpdatedTime(0)
                .build();
        }
    }

    private EntityDTO.Builder entityBuilder() {
        return EntityDTO.newBuilder()
            .setId("target-entity")
            .setDisplayName("target-entity")
            .setEntityType(EntityType.VIRTUAL_MACHINE);
    }
}
