package com.vmturbo.topology.processor.stitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.stitchingData;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.topologyMapOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyEntityTopologyGraphCreator;

public class StitchingContextTest {
    private final TargetStore targetStore = mock(TargetStore.class);

    private final StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(8, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
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
        when(targetStore.getAll()).thenReturn(Collections.emptyList());

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
        assertThat(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, 1L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder()));
        assertThat(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, 2L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e3_2.getEntityDtoBuilder(), e4_2.getEntityDtoBuilder()));
    }

    @Test
    public void testInternalEntitiesForTarget() {
        assertThat(stitchingContext.internalEntities(1L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder()));
        assertThat(stitchingContext.internalEntities(2L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e3_2.getEntityDtoBuilder(), e4_2.getEntityDtoBuilder()));
        assertThat(stitchingContext.internalEntities(3L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testExternalEntities() {
        assertThat(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, 1L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e3_2.getEntityDtoBuilder(), e4_2.getEntityDtoBuilder()));
        assertThat(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, 2L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder()));
    }

    @Test
    public void testGetEntitiesOfType() {
        assertThat(stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            containsInAnyOrder(e1_1.getEntityDtoBuilder(), e2_1.getEntityDtoBuilder(),
            e3_2.getEntityDtoBuilder(), e4_2.getEntityDtoBuilder()));
        assertThat(stitchingContext.getEntitiesOfType(EntityType.PHYSICAL_MACHINE)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testRemove() {
        final TopologyStitchingEntity toRemove = stitchingContext
            .getEntity(e2_1.getEntityDtoBuilder()).get();
        boolean removed = stitchingContext.removeEntity(toRemove);

        assertTrue(removed);
        assertEquals(3, stitchingContext.size());

        assertThat(stitchingContext.internalEntities(EntityType.VIRTUAL_MACHINE, 1L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            contains(e1_1.getEntityDtoBuilder()));
        assertThat(stitchingContext.externalEntities(EntityType.VIRTUAL_MACHINE, 2L)
                .map(TopologyStitchingEntity::getEntityBuilder)
                .collect(Collectors.toList()),
            contains(e1_1.getEntityDtoBuilder()));

        assertThat(stitchingContext.getEntity(e1_1.getEntityDtoBuilder()).get()
                .getConsumers(),
            is(empty())
        );

        // Removing an already removed entity should ail
        assertFalse(stitchingContext.removeEntity(toRemove));
    }

    @Test
    public void testConstructTopology() {
        final TopologyGraph<TopologyEntity> topology = TopologyEntityTopologyGraphCreator.newGraph(stitchingContext.constructTopology());
        assertEquals(4, topology.size());

        assertEquals(e1_1.getOid(), topology.getEntity(e1_1.getOid()).get().getOid());
        DiscoveryOrigin origin = topology.getEntity(e1_1.getOid()).get().getDiscoveryOrigin().get();
        assertEquals(e1_1.getLastUpdatedTime(), origin.getLastUpdatedTime());
        assertEquals(1, origin.getDiscoveredTargetDataMap().size());
        assertEquals(e1_1.getTargetId(),
                     origin.getDiscoveredTargetDataMap().keySet().iterator().next().longValue());
        assertEquals(1, topology.getConsumers(e1_1.getOid()).count());
        assertEquals(0, topology.getProviders(e1_1.getOid()).count());

        assertEquals(0, topology.getConsumers(e2_1.getOid()).count());
        assertEquals(1, topology.getProviders(e2_1.getOid()).count());

        assertEquals(1, topology.getConsumers(e3_2.getOid()).count());
        assertEquals(0, topology.getProviders(e3_2.getOid()).count());

        assertEquals(0, topology.getConsumers(e4_2.getOid()).count());
        assertEquals(1, topology.getProviders(e4_2.getOid()).count());
    }

    // TODO: Remove this test after adding shared storage support and eliminating collision resolution.
    @Test
    public void testConstructTopologyDuplicateOids() {
        final StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(8, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

        stitchingContextBuilder.addEntity(e1_1, target1Graph);
        final StitchingEntityData e1_2DuplicateOid = new StitchingEntityData(e3_2.getEntityDtoBuilder(),
            e1_1.getTargetId() + 1, e1_1.getOid(), 0, false);

        stitchingContextBuilder.addEntity(e1_1, target1Graph);
        stitchingContextBuilder.addEntity(e1_2DuplicateOid, topologyMapOf(e1_2DuplicateOid));
        final StitchingContext stitchingContext = stitchingContextBuilder.build();

        assertEquals(1, stitchingContext.constructTopology().size());
    }

    @Test
    public void testConstructTopologyWithDuplicateLocalIds() {
        stitchingContextBuilder.addEntity(e1_1, target1Graph);
        stitchingContextBuilder.addEntity(e2_1, target1Graph);
        stitchingContextBuilder.addEntity(e3_2, target2Graph);
        stitchingContextBuilder.addEntity(e4_2, target2Graph);

        // Create 2 entities with the same id ("1") discovered by two different targets.
        // We should still be able to successfully construct a topology.
        final StitchingEntityData e1_3_duplicate = stitchingData("1", Collections.emptyList()).forTarget(3L);
        stitchingContextBuilder.addEntity(e1_3_duplicate, ImmutableMap.of("1", e1_3_duplicate));

        assertEquals(5, stitchingContextBuilder.build().constructTopology().size());
    }
}