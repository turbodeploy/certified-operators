package com.vmturbo.market.runner.cost;

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;

/**
 * Tests for MarketCloudCostDataProvider.
 */
public class MarketCloudCostDataProviderTest {

    /**
     * An entity is covered by 2 RIs, but they are not in scope.
     * Test that MarketCloudCostDataProvider.filterCouponsCoveredByRi filters out these RIs because
     * they are not in scope.
     */
    @Test
    public void testFilterCouponsCoveredByRi() {
        Map<Long, EntityReservedInstanceCoverage> coverageMap = Maps.newHashMap();
        coverageMap.put(100L, EntityReservedInstanceCoverage.newBuilder().setEntityId(100L)
            .putCouponsCoveredByRi(1L, 16)
            .putCouponsCoveredByRi(2L, 32).build());
        Set<Long> riBoughtIds = Sets.newHashSet();

        Map<Long, EntityReservedInstanceCoverage> filteredCoverageMap =
            MarketCloudCostDataProvider.filterCouponsCoveredByRi(coverageMap, riBoughtIds);

        assertTrue(filteredCoverageMap.get(100L).getCouponsCoveredByRiMap().isEmpty());
    }
}