package com.vmturbo.components.common.stats;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
    public static final Map<String, ApiEntityType> COUNT_ENTITY_METRIC_NAMES =
        ImmutableBiMap.<String, ApiEntityType>builder()
            .put(StringConstants.NUM_HOSTS, ApiEntityType.PHYSICAL_MACHINE)
            .put(StringConstants.NUM_STORAGES, ApiEntityType.STORAGE)
            .put(StringConstants.NUM_CONTAINERS, ApiEntityType.CONTAINER)
            .put(StringConstants.NUM_VDCS, ApiEntityType.VIRTUAL_DATACENTER)
            .put(StringConstants.NUM_VIRTUAL_DISKS, ApiEntityType.VIRTUAL_VOLUME)
            .put(StringConstants.NUM_VMS, ApiEntityType.VIRTUAL_MACHINE)
            .put(StringConstants.NUM_DBSS, ApiEntityType.DATABASE_SERVER)
            .put(StringConstants.NUM_DBS, ApiEntityType.DATABASE)
            .put(StringConstants.NUM_DAS, ApiEntityType.DISKARRAY)
            .put(StringConstants.NUM_LOADBALANCERS, ApiEntityType.LOAD_BALANCER)
            .put(StringConstants.NUM_DCS, ApiEntityType.DATACENTER)
            .put(StringConstants.NUM_APPS, ApiEntityType.APPLICATION_COMPONENT)
            .put(StringConstants.NUM_SERVICES, ApiEntityType.SERVICE)
            .put(StringConstants.NUM_NETWORKS, ApiEntityType.NETWORK)
            .build();

    /**
     * Set of sdk entity types which don't have saved priceIndex. If any of these entity types
     * starts to have priceIndex, it should be removed from this set. One possible candidate is
     * VIRTUAL_VOLUME, but that will not happen until the huge storage model refactor in XL.
     */
    public static final Set<Integer> SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES =
            ImmutableSet.<Integer>builder()
                    .add(EntityType.NETWORK.getNumber())
                    .add(EntityType.INTERNET.getNumber())
                    .add(EntityType.VIRTUAL_VOLUME.getNumber())
                    .add(EntityType.HYPERVISOR_SERVER.getNumber())
                    .add(EntityType.REGION.getNumber())
                    .add(EntityType.AVAILABILITY_ZONE.getNumber())
                    .add(EntityType.BUSINESS_ACCOUNT.getNumber())
                    .build();
}
