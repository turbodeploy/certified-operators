package com.vmturbo.common.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityBatch;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * Unit tests for {@link RepositoryDTOUtil}.
 */
public class RepositoryDTOUtilTest {

    @Test
    public void testUnplacedFilterMatch() {
        final TopologyEntityDTO unplacedEntity = newEntity()
                // Unplaced commodity - no provider ID.
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder())
                .build();
        assertTrue(RepositoryDTOUtil.entityMatchesFilter(unplacedEntity, TopologyEntityFilter.newBuilder()
                .setUnplacedOnly(true)
                .build()));
    }

    @Test
    public void testUnplacedFilterNoMatch() {
        final TopologyEntityDTO placedEntity = newEntity()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(7L))
                .build();
        // Make sure the entity matches without unplacedOnly
        assertTrue(RepositoryDTOUtil.entityMatchesFilter(placedEntity,
                TopologyEntityFilter.getDefaultInstance()));
        // And doesn't match with unplacedOnly
        assertFalse(RepositoryDTOUtil.entityMatchesFilter(placedEntity, TopologyEntityFilter.newBuilder()
                .setUnplacedOnly(true)
                .build()));
    }

    private List<EntityBatch> entityBatches = Arrays.asList(
            EntityBatch.newBuilder()
                .addEntities(TopologyEntityDTO.newBuilder()
                    .setOid(1)
                    .setEntityType(EntityType.VIRTUAL_MACHINE.getValue()))
                .addEntities(TopologyEntityDTO.newBuilder()
                        .setOid(2)
                        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue()))
                .build(),
            EntityBatch.newBuilder()
                .addEntities(TopologyEntityDTO.newBuilder()
                    .setOid(3)
                    .setEntityType(EntityType.VIRTUAL_MACHINE.getValue()))
                .build(),
            EntityBatch.getDefaultInstance()); // empty batch

    @Test
    public void testTopologyEntityStream() {
        List<TopologyEntityDTO> entities = RepositoryDTOUtil.topologyEntityStream(entityBatches.iterator())
                .collect(Collectors.toList());
        assertEquals(3, entities.size());
    }

    @Nonnull
    private TopologyEntityDTO.Builder newEntity() {
        return TopologyEntityDTO.newBuilder()
                    .setEntityType(10)
                    .setOid(11L);
    }
}
