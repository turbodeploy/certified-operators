package com.vmturbo.topology.processor.history;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO;

/**
 * Tests the {@link EntityCommodityFieldReference}.
 */
public class EntityCommodityFieldReferenceTest {
    private static final long ENTITY_ID = 1L;
    private static final long LIVE_ENTITY_ID = 10L;
    private static final long PROVIDER_ENTITY_ID = 100L;
    private static final long LIVE_PROVIDER_ENTITY_ID = 101L;
    private static final TopologyDTO.CommodityType COMMODITY_TYPE = TopologyDTO.CommodityType
        .newBuilder().setType(5).build();
    private static final CommodityField COMMODITY_FIELD = CommodityField.USED;
    private HistoryAggregationContext context;

    /**
     * Setups the test environment.
     */
    @Before
    public void setUp() {
        context = mock(HistoryAggregationContext.class);
        when(context.isPlan()).thenReturn(true);
        when(context.getClonedFromEntityOid(null)).thenReturn(Optional.empty());
        when(context.getClonedFromEntityOid(ENTITY_ID)).thenReturn(Optional.of(LIVE_ENTITY_ID));
        when(context.getClonedFromEntityOid(LIVE_ENTITY_ID)).thenReturn(Optional.of(LIVE_ENTITY_ID));
        when(context.getClonedFromEntityOid(PROVIDER_ENTITY_ID)).thenReturn(Optional.of(LIVE_PROVIDER_ENTITY_ID));
        when(context.getClonedFromEntityOid(LIVE_PROVIDER_ENTITY_ID)).thenReturn(Optional.of(LIVE_PROVIDER_ENTITY_ID));
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which does not
     * have provider and the referencing entity is a clone of another entity.
     */
    @Test
    public void testGetLiveTopologyFieldReferenceClonedNoProvider() {
        // ARRANGE
        EntityCommodityFieldReference commodityReference = new EntityCommodityFieldReference(ENTITY_ID,
            COMMODITY_TYPE, COMMODITY_FIELD);

        // ACT
        EntityCommodityFieldReference liveCommodityReference =
            commodityReference.getLiveTopologyFieldReference(context);
        // doing it second time to make sure that we don't create an object second time
        EntityCommodityFieldReference liveCommodityReference2 =
            commodityReference.getLiveTopologyFieldReference(context);

        // ASSERT
        assertThat(liveCommodityReference2, Matchers.sameInstance(liveCommodityReference2));
        assertThat(liveCommodityReference.getEntityOid(), is(LIVE_ENTITY_ID));
        assertThat(liveCommodityReference.getCommodityType(), is(COMMODITY_TYPE));
        assertThat(liveCommodityReference.getField(), is(COMMODITY_FIELD));
        assertNull(liveCommodityReference.getProviderOid());
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which does not have
     * provider and the referencing entity is a live topology entity.
     */
    @Test
    public void testGetLiveTopologyFieldReferenceNotClonedNoProvider() {
        // ARRANGE
        EntityCommodityFieldReference commodityReference = new EntityCommodityFieldReference(LIVE_ENTITY_ID,
            COMMODITY_TYPE, COMMODITY_FIELD);

        // ACT
        EntityCommodityFieldReference liveCommodityReference =
            commodityReference.getLiveTopologyFieldReference(context);

        // ASSERT
        assertThat(liveCommodityReference, Matchers.sameInstance(commodityReference));
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which has a
     * provider and the referencing entity is a live topology entity.
     */
    @Test
    public void testGetLiveTopologyFieldReferenceNotClonedWithProvider() {
        // ARRANGE
        EntityCommodityFieldReference commodityReference = new EntityCommodityFieldReference(LIVE_ENTITY_ID,
            COMMODITY_TYPE, LIVE_PROVIDER_ENTITY_ID, COMMODITY_FIELD);

        // ACT
        EntityCommodityFieldReference liveCommodityReference =
            commodityReference.getLiveTopologyFieldReference(context);

        // ASSERT
        assertThat(liveCommodityReference, Matchers.sameInstance(commodityReference));
    }

    /**
     * Test the case when we try to get the commodity reference for a commodity which has a
     * provider and the referencing provider entity is a clone of another entity.
     */
    @Test
    public void testGetLiveTopologyFieldReferenceClonedWithProvider() {
        // ARRANGE
        EntityCommodityFieldReference commodityReference = new EntityCommodityFieldReference(LIVE_ENTITY_ID,
            COMMODITY_TYPE, PROVIDER_ENTITY_ID, COMMODITY_FIELD);

        // ACT
        EntityCommodityFieldReference liveCommodityReference =
            commodityReference.getLiveTopologyFieldReference(context);

        // ASSERT
        assertThat(liveCommodityReference.getEntityOid(), is(LIVE_ENTITY_ID));
        assertThat(liveCommodityReference.getCommodityType(), is(COMMODITY_TYPE));
        assertThat(liveCommodityReference.getField(), is(COMMODITY_FIELD));
        assertThat(liveCommodityReference.getProviderOid(), is(LIVE_PROVIDER_ENTITY_ID));
    }
}
