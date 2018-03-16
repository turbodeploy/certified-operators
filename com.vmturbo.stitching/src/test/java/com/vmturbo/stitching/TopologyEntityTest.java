package com.vmturbo.stitching;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
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