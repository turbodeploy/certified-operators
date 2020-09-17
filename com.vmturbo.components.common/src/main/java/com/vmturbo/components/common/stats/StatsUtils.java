package com.vmturbo.components.common.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_DISK;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class StatsUtils {

    private StatsUtils() {
    }

    /**
     * stat filters used to filter commodity stats by provider type.
     */
    public static final String PROVIDER_TYPE_STAT_FILTER = "providerType";
    /**
     * stat filters used to filter commodity stats by provider.
     */
    public static final String PROJECTED_PROVIDER_STAT_FILTER = "projectedProvider";

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
     * Return Map from commodity name to provider type for which the commodities are being
     * requested.
     *
     * @param statsFilter for which the Map is being returned.
     * @return map from commodity name to provider type.
     */
    public static Map<String, Set<Integer>> commodityNameToProviderType(
        @Nonnull StatsFilter statsFilter) {
        return statsFilter.getCommodityRequestsList().stream()
            .collect(Collectors.toMap(CommodityRequest::getCommodityName,
                request -> extractProviderType(request.getPropertyValueFilterList())));
    }

    /**
     * Return Map from commodity name to groupBy fields specific in the statsFilter.
     *
     * @param statsFilter for which the map is being created.
     * @return map from commodity name to groupBy fields.
     */
    public static Map<String, Set<String>> commodityNameToGroupByFilters(
        @Nonnull StatsFilter statsFilter) {
        return statsFilter
            .getCommodityRequestsList().stream()
            .filter(CommodityRequest::hasCommodityName)
            .collect(Collectors.toMap(CommodityRequest::getCommodityName,
                commodityRequest -> commodityRequest.getGroupByList().stream()
                    .filter(Objects::nonNull)
                    // Only support "key" and "virtualDisk" group by for now
                    // Both equate to grouping by the key (matches existing logic in
                    // History component).
                    .filter(groupBy -> KEY.equalsIgnoreCase(groupBy)
                        || VIRTUAL_DISK.equalsIgnoreCase(groupBy))
                    .collect(Collectors.toSet())
            ));
    }

    private static Set<Integer> extractProviderType(
        final List<PropertyValueFilter> propertyValueFilterList) {
        return propertyValueFilterList.stream()
            .filter(property -> PROVIDER_TYPE_STAT_FILTER.equals(property.getProperty()))
            .map(property -> ApiEntityType.fromString(property.getValue()).typeNumber())
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
                    .add(EntityType.CLOUD_SERVICE.getNumber())
                    .add(EntityType.COMPUTE_TIER.getNumber())
                    .add(EntityType.DATABASE_TIER.getNumber())
                    .add(EntityType.DATABASE_SERVER_TIER.getNumber())
                    .add(EntityType.STORAGE_TIER.getNumber())
                    .add(EntityType.LOAD_BALANCER.getNumber())
                    .add(EntityType.CONTAINER_SPEC.getNumber())
                    .add(EntityType.SERVICE_PROVIDER.getNumber())
                    .build();
}
