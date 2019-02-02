package com.vmturbo.common.protobuf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;

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

    @Nonnull
    private TopologyEntityDTO.Builder newEntity() {
        return TopologyEntityDTO.newBuilder()
                    .setEntityType(10)
                    .setOid(11L);
    }
}
