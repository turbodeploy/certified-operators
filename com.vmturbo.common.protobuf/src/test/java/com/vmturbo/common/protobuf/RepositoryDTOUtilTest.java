package com.vmturbo.common.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
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

    /**
     * Test filter with different entity types.
     */
    @Test
    public void testEntityTypeFilterTest() {
        final TopologyEntityDTO vmEntity = newEntity()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder())
                .build();

        assertTrue(RepositoryDTOUtil.entityMatchesFilter(vmEntity, TopologyEntityFilter.newBuilder()
                .addAllEntityTypes(ImmutableList.of(EntityType.VIRTUAL_MACHINE.getValue()))
                .build()));

        assertFalse(RepositoryDTOUtil.entityMatchesFilter(vmEntity, TopologyEntityFilter.newBuilder()
                .addAllEntityTypes(ImmutableList.of(EntityType.PHYSICAL_MACHINE.getValue()))
                .build()));
    }

    /**
     * Test filter with unplaced false and entityType.
     */
    @Test
    public void testMultipleFilterMatch() {
        final TopologyEntityDTO placedEntity = newEntity()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(7L))
                .build();

        // Matched because entity is placed VM
        assertTrue(RepositoryDTOUtil.entityMatchesFilter(placedEntity, TopologyEntityFilter.newBuilder()
                .setUnplacedOnly(false)
                .addAllEntityTypes(ImmutableList.of(EntityType.VIRTUAL_MACHINE.getValue()))
                .build()));
    }

    /**
     * Test filter with unplaced true and entityType.
     */
    @Test
    public void testMultipleFilterNoMatch() {
        final TopologyEntityDTO placedEntity = newEntity()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(7L))
                .build();

        // No match because entity is placed VM.
        assertFalse(RepositoryDTOUtil.entityMatchesFilter(placedEntity, TopologyEntityFilter.newBuilder()
                .setUnplacedOnly(true)
                .addAllEntityTypes(ImmutableList.of(EntityType.VIRTUAL_MACHINE.getValue()))
                .build()));
    }

    private List<PartialEntityBatch> entityBatches = Arrays.asList(
            PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder()
                    .setFullEntity(TopologyEntityDTO.newBuilder()
                        .setOid(1)
                        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())))
                .addEntities(PartialEntity.newBuilder()
                    .setFullEntity(TopologyEntityDTO.newBuilder()
                        .setOid(2)
                        .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())))
                .build(),
            PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder()
                    .setFullEntity(TopologyEntityDTO.newBuilder()
                    .setOid(3)
                    .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())))
                .build(),
            PartialEntityBatch.getDefaultInstance()); // empty batch

    @Test
    public void testTopologyEntityStream() {
        List<PartialEntity> entities = RepositoryDTOUtil.topologyEntityStream(entityBatches.iterator())
                .collect(Collectors.toList());
        assertEquals(3, entities.size());
    }

    @Nonnull
    private TopologyEntityDTO.Builder newEntity() {
        return TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE.getValue())
                    .setOid(11L);
    }
}
