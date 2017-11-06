package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.topologyMapOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.topology.processor.topology.TopologyGraph;

public class StitchingContextTest {
    private final StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(8);
    private StitchingContext stitchingContext;

    @BeforeClass
    public static void initIdentityGenerator() {
        IdentityGenerator.initPrefix(0);
    }

    private final StitchingEntityData e1_1 = stitchingData("1", Collections.emptyList()).forTarget(1L);
    private final StitchingEntityData e2_1 = stitchingData("2", Collections.singletonList("1")).forTarget(1L);
    private final StitchingEntityData e3_2 = stitchingData("3", Collections.emptyList()).forTarget(2L);
    private final StitchingEntityData e4_2 = stitchingData("4", Collections.singletonList("3")).forTarget(2L);
    private final Map<String, StitchingEntityData> target1Graph = topologyMapOf(e1_1, e2_1);
    private final Map<String, StitchingEntityData> target2Graph = topologyMapOf(e3_2, e4_2);

    @Before
    public void setup() {
        //  2     4
        //  |     |
        //  1     3
        stitchingContextBuilder.addEntity(e1_1, target1Graph);
        stitchingContextBuilder.addEntity(e2_1, target1Graph);
        stitchingContextBuilder.addEntity(e3_2, target2Graph);
        stitchingContextBuilder.addEntity(e4_2, target2Graph);

        stitchingContext = stitchingContextBuilder.build();
    }

    @Test
    public void testSize() {
        assertEquals(4, stitchingContext.size());
    }

    @Test
    public void testInternalEntities() {
        assertThat(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, 1L).collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder()));
        assertThat(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, 2L).collect(Collectors.toList()),
            containsInAnyOrder(e3_2.getEntityDtoBuilder(), e4_2.getEntityDtoBuilder()));
    }

    @Test
    public void testExternalEntities() {
        assertThat(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, 1L).collect(Collectors.toList()),
            containsInAnyOrder(e3_2.getEntityDtoBuilder(), e4_2.getEntityDtoBuilder()));
        assertThat(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, 2L).collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder()));
    }

    @Test
    public void testApplyRemoval() {
        final StitchingOperationResult result = StitchingOperationResult.newBuilder()
            .removeEntity(e2_1.getEntityDtoBuilder())
            .build();

        stitchingContext.applyStitchingResult(result);
        assertEquals(3, stitchingContext.size());

        assertThat(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, 1L).collect(Collectors.toList()),
            contains(e1_1.getEntityDtoBuilder()));
        assertThat(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, 2L).collect(Collectors.toList()),
            contains(e1_1.getEntityDtoBuilder()));
        assertThat(stitchingContext.getStitchingGraph()
            .getConsumerEntities(e1_1.getEntityDtoBuilder())
            .collect(Collectors.toList()),
            is(empty())
        );
    }

    @Test
    public void testApplyUpdate() {
        // Have entity2 also buy from entity3
        final StitchingOperationResult result = StitchingOperationResult.newBuilder()
            .updateCommoditiesBought(e2_1.getEntityDtoBuilder(), entity ->
                entity.addCommoditiesBought(CommodityBought.newBuilder().setProviderId(e3_2.getLocalId())))
            .build();

        final TopologyStitchingGraph graph = stitchingContext.getStitchingGraph();
        assertThat(graph.getConsumerEntities(e3_2.getEntityDtoBuilder()).collect(Collectors.toList()),
            contains(e4_2.getEntityDtoBuilder()));
        assertThat(graph.getProviderEntities(e2_1.getEntityDtoBuilder()).collect(Collectors.toList()),
            contains(e1_1.getEntityDtoBuilder()));

        stitchingContext.applyStitchingResult(result);
        assertEquals(4, stitchingContext.size());

        assertThat(graph.getConsumerEntities(e3_2.getEntityDtoBuilder()).collect(Collectors.toList()),
            containsInAnyOrder(e4_2.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder()));
        assertThat(graph.getProviderEntities(e2_1.getEntityDtoBuilder()).collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e3_2.getEntityDtoBuilder()));
    }

    @Test
    public void testConstructTopology() {
        final TopologyGraph topology = stitchingContext.constructTopology();
        assertEquals(4, topology.vertexCount());

        assertEquals(e1_1.getOid(), topology.getVertex(e1_1.getOid()).get().getOid());
        assertEquals(1, topology.getConsumers(e1_1.getOid()).count());
        assertEquals(0, topology.getProducers(e1_1.getOid()).count());

        assertEquals(0, topology.getConsumers(e2_1.getOid()).count());
        assertEquals(1, topology.getProducers(e2_1.getOid()).count());

        assertEquals(1, topology.getConsumers(e3_2.getOid()).count());
        assertEquals(0, topology.getProducers(e3_2.getOid()).count());

        assertEquals(0, topology.getConsumers(e4_2.getOid()).count());
        assertEquals(1, topology.getProducers(e4_2.getOid()).count());
    }
}