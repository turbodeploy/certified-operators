package com.vmturbo.market.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class MarketTierTest {

    @Test
    public void testHashCodeAndEquals() {
         TopologyEntityDTO computeTier1 =  TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setOid(11)
                .build();
        TopologyEntityDTO region1 =  TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(1)
                .build();
        Map<MarketTier, Integer> mstMap = Maps.newHashMap();
        MarketTier mst1 = new OnDemandMarketTier(computeTier1, region1);
        MarketTier mst2 = new OnDemandMarketTier(computeTier1, region1);
        mstMap.put(mst1, 1);

        assertFalse(mst1 == mst2);
        assertTrue(mst1.equals(mst2));
        assertTrue(mst1.hashCode() == mst2.hashCode());
        assertEquals(mstMap.get(mst2), (Integer)1);
    }
}