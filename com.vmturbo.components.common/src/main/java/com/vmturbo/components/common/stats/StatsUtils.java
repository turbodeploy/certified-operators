package com.vmturbo.components.common.stats;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;

public class StatsUtils {

    /**
     * Collect just the stats names to fetch from the list of commodities listed in this filter.
     *
     * @param statsFilter The {@link StatsFilter} with a list of commodity requests
     * @return a set of just the commodity names from each commodity request in the stats filter
     */
    @Nonnull
    public static Set<String> collectCommodityNames(@Nonnull final StatsFilter statsFilter) {
        return statsFilter.getCommodityRequestsList().stream()
                .filter(StatsFilter.CommodityRequest::hasCommodityName)
                .map(StatsFilter.CommodityRequest::getCommodityName)
                .collect(Collectors.toSet());
    }
}