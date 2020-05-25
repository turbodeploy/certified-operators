package com.vmturbo.stitching;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.Objects;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;

/**
 * Tests the {@link EntityCommodityReference} class.
 */
public class EntityCommodityReferenceTest {
    private static final long ENTITY_ID = 1L;
    private static final long LIVE_ENTITY_ID = 10L;
    private static final long PROVIDER_ENTITY_ID = 100L;
    private static final long LIVE_PROVIDER_ENTITY_ID = 101L;
    private static final TopologyDTO.CommodityType COMMODITY_TYPE = TopologyDTO.CommodityType
        .newBuilder().setType(5).build();

    /**
     * Test the case when we try to get the commodity reference for a commodity which does not
     * have provider and the referencing entity is a clone of another entity.
     */
    @Test
    public void testGetLiveTopologyCommodityReferenceClonedNoProvider() {
        // ARRANGE
        EntityCommodityReference commodityReference = new EntityCommodityReference(ENTITY_ID,
            COMMODITY_TYPE, null);

        // ACT
        EntityCommodityReference liveCommodityReference =
            commodityReference.getLiveTopologyCommodityReference(this::getLiveEntityId);

        // ASSERT
        assertThat(liveCommodityReference.getEntityOid(), is(LIVE_ENTITY_ID));
        assertThat(liveCommodityReference.getCommodityType(), is(COMMODITY_TYPE));
        assertNull(liveCommodityReference.getProviderOid());
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which does not have
     * provider and the referencing entity is a live topology entity.
     */
    @Test
    public void testGetLiveTopologyCommodityReferenceNotClonedNoProvider() {
        // ARRANGE
        EntityCommodityReference commodityReference = new EntityCommodityReference(LIVE_ENTITY_ID,
            COMMODITY_TYPE, null);

        // ACT
        EntityCommodityReference liveCommodityReference =
            commodityReference.getLiveTopologyCommodityReference(this::getLiveEntityId);

        // ASSERT
        assertThat(liveCommodityReference, Matchers.sameInstance(commodityReference));
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which has a
     * provider and the referencing entity is a live topology entity.
     */
    @Test
    public void testGetLiveTopologyCommodityReferenceNotClonedWithProvider() {
        // ARRANGE
        EntityCommodityReference commodityReference = new EntityCommodityReference(LIVE_ENTITY_ID,
            COMMODITY_TYPE, LIVE_PROVIDER_ENTITY_ID);

        // ACT
        EntityCommodityReference liveCommodityReference =
            commodityReference.getLiveTopologyCommodityReference(this::getLiveEntityId);

        // ASSERT
        assertThat(liveCommodityReference, Matchers.sameInstance(commodityReference));
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which has a
     * provider and the referencing provider entity is a clone of another entity.
     */
    @Test
    public void testGetLiveTopologyCommodityReferenceClonedWithProvider() {
        // ARRANGE
        EntityCommodityReference commodityReference = new EntityCommodityReference(LIVE_ENTITY_ID,
            COMMODITY_TYPE, PROVIDER_ENTITY_ID);

        // ACT
        EntityCommodityReference liveCommodityReference =
            commodityReference.getLiveTopologyCommodityReference(this::getLiveEntityId);

        // ASSERT
        assertThat(liveCommodityReference.getEntityOid(), is(LIVE_ENTITY_ID));
        assertThat(liveCommodityReference.getCommodityType(), is(COMMODITY_TYPE));
        assertThat(liveCommodityReference.getProviderOid(), is(LIVE_PROVIDER_ENTITY_ID));
    }

    private Optional<Long> getLiveEntityId(Long id) {
        if (id == null) {
            return Optional.empty();
        }
        if (Objects.equals(id, ENTITY_ID) || Objects.equals(id, LIVE_ENTITY_ID)) {
            return Optional.of(LIVE_ENTITY_ID);
        }
        if (Objects.equals(id, PROVIDER_ENTITY_ID) || Objects.equals(id, LIVE_PROVIDER_ENTITY_ID)) {
            return Optional.of(LIVE_PROVIDER_ENTITY_ID);
        }
        return Optional.empty();
    }
}