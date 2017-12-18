package com.vmturbo.stitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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
        assertFalse(entity.getDiscoveryInformation().isPresent());
    }

    @Test
    public void testBuildDiscoveryInformation() {
        final TopologyEntity entity = TopologyEntity.newBuilder(dtoBuilder)
            .discoveryInformation(TopologyEntity.discoveredBy(111L).lastUpdatedAt(222L))
            .build();

        assertTrue(entity.getDiscoveryInformation().isPresent());
        assertEquals(111L, entity.getDiscoveryInformation().get().getTargetId());
        assertEquals(222L, entity.getDiscoveryInformation().get().getLastUpdatedTime());
    }
}