package com.vmturbo.market.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class MarketTierTest {

    private TopologyEntityDTO computeTier;
    private TopologyEntityDTO storageTier;
    private TopologyEntityDTO region;

    /**
     * Setup for the test
     */
    @Before
    public void setup() {
        computeTier =  TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(11)
                .build();
        storageTier = TopologyEntityDTO.newBuilder().setEntityType(EntityType.STORAGE_TIER_VALUE)
                .setOid(12).build();
        region =  TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(1)
                .build();
    }

    /**
     * Test the overriden Hash code and equals for on demand market tier.
     */
    @Test
    public void testHashCodeAndEqualsforOnDemandMarketTier() {

        Map<MarketTier, Integer> mstMap = Maps.newHashMap();
        MarketTier mst1 = new OnDemandMarketTier(computeTier);
        MarketTier mst2 = new OnDemandMarketTier(computeTier);
        mstMap.put(mst1, 1);

        assertFalse(mst1 == mst2);
        assertTrue(mst1.equals(mst2));
        assertTrue(mst1.hashCode() == mst2.hashCode());
        assertEquals((Integer)1, mstMap.get(mst2));
    }

    /**
     * Test the overriden Hash code and equals for on demand storage tier.
     */
    @Test
    public void testHashCodeAndEqualsforOnDemandStorageTier() {
        Map<MarketTier, Integer> mstMap = Maps.newHashMap();
        MarketTier mst1 = new OnDemandMarketTier(storageTier);
        MarketTier mst2 = new OnDemandMarketTier(storageTier);
        mstMap.put(mst1, 1);

        assertFalse(mst1 == mst2);
        assertTrue(mst1.equals(mst2));
        assertTrue(mst1.hashCode() == mst2.hashCode());
        assertEquals((Integer)1, mstMap.get(mst2));
    }

    @Test
    /**
     * Testing for false ri discount on storage tier.
     */
    public void testOnDemandStorageTierHasRiCoverage() {
        MarketTier mst1 = new OnDemandMarketTier(storageTier);
        assertEquals(false, mst1.hasRIDiscount());
    }
}
