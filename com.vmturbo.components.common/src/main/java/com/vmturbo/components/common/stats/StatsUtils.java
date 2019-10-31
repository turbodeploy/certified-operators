package com.vmturbo.components.common.stats;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

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

    /**
     * Mapping between a request of stats and the corresponding entity type.
     */
    public static final Map<String, UIEntityType> COUNT_ENTITY_METRIC_NAMES =
        ImmutableBiMap.<String, UIEntityType>builder()
            .put(StringConstants.NUM_HOSTS, UIEntityType.PHYSICAL_MACHINE)
            .put(StringConstants.NUM_STORAGES, UIEntityType.STORAGE)
            .put(StringConstants.NUM_CONTAINERS, UIEntityType.CONTAINER)
            .put(StringConstants.NUM_VDCS, UIEntityType.VIRTUAL_DATACENTER)
            .put(StringConstants.NUM_VIRTUAL_DISKS, UIEntityType.VIRTUAL_VOLUME)
            .put(StringConstants.NUM_VMS, UIEntityType.VIRTUAL_MACHINE)
            .put(StringConstants.NUM_DBSS, UIEntityType.DATABASE_SERVER)
            .put(StringConstants.NUM_DBS, UIEntityType.DATABASE)
            .put(StringConstants.NUM_DAS, UIEntityType.DISKARRAY)
            .put(StringConstants.NUM_LOADBALANCERS, UIEntityType.LOAD_BALANCER)
            .put(StringConstants.NUM_DCS, UIEntityType.DATACENTER)
            .put(StringConstants.NUM_APPS, UIEntityType.APPLICATION)
            .put(StringConstants.NUM_VAPPS, UIEntityType.VIRTUAL_APPLICATION)
            .put(StringConstants.NUM_NETWORKS, UIEntityType.NETWORK)
            .build();
}
