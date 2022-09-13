package com.vmturbo.api.component.external.api.util.stats;

import static com.vmturbo.components.common.ClassicEnumMapper.COMMODITY_TYPE_MAPPINGS;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.CachedEntityInfo;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.ProbeUseCase;
import com.vmturbo.platform.sdk.common.util.ProbeUseCaseUtil;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Util to help filter out invalid/hardcoded power and cooling commodities.
 */
public class SustainabilityCommodityFilterUtils {

    private SustainabilityCommodityFilterUtils() {}

    /**
     * This serves as a list of whitelisted entity types to show power/cooling metrics for.
     */
    public static final ImmutableSet<ApiEntityType> ENTITIES_WITH_POWER_COOLING_METRICS = ImmutableSet.of(
            ApiEntityType.VIRTUAL_MACHINE, ApiEntityType.PHYSICAL_MACHINE, ApiEntityType.DATACENTER,
            ApiEntityType.CHASSIS, ApiEntityType.IOMODULE);

    /**
     * Set of power and cooling commodity types.
     */
    public static final ImmutableSet<CommodityType> POWER_AND_COOLING =
            ImmutableSet.of(CommodityType.POWER, CommodityType.COOLING);

    /**
     * Set of Host of DC entity types.
     */
    public static final ImmutableSet<ApiEntityType> HOST_AND_DC =
            ImmutableSet.of(ApiEntityType.PHYSICAL_MACHINE, ApiEntityType.DATACENTER);

    /**
     * Check if we should enable showing of cooling and power commodities. We whitelist
     * the power and cooling only for certain entities.
     *
     * @param scope scope to check
     * @param context context for the stats query
     * @return true if allowing showing cooling and power commodities for the scope, otherwise false
     */
    protected static boolean allowCoolingPowerCommodities(@Nonnull ApiId scope, @Nonnull StatsQueryContext context) {
        final Set<Long> fabricTargets = context.getTargets().stream()
                .filter(t -> ProbeUseCaseUtil.isUseCaseSupported(
                        ProbeCategory.create(t.probeInfo().category()),
                        ProbeUseCase.ALLOW_FABRIC_COMMODITIES))
                .map(ThinTargetInfo::oid)
                .collect(Collectors.toSet());
        if (scope.isGroup() || scope.isEntity()) {
            Optional<Set<ApiEntityType>> entityTypes = scope.getScopeTypes();
            if (!entityTypes.isPresent() || entityTypes.get().isEmpty()) {
                return false;
            }
            // Whitelisted entities that include power metrics
            return entityTypes.get().stream().anyMatch(ENTITIES_WITH_POWER_COOLING_METRICS::contains);
        } else if (scope.isTarget()) {
            // if it's target, just check if it's fabric target
            return fabricTargets.contains(scope.oid());
        } else if (scope.isRealtimeMarket() || scope.isPlan()) {
            // do not show for plan or market
            return false;
        }
        return false;
    }

    /**
     * Filter out power and cooling commodities that are sold by datacenters. Datacenters sell hard
     * coded values hence we hide them.
     *
     * @param stats List of stats snapshots
     * @param scope scope of entities
     */
    protected static void filterOutFakePowerAndCoolingMetrics(
            @Nonnull List<StatSnapshotApiDTO> stats, @Nonnull ApiId scope,
            @Nonnull UuidMapper uuidMapper) {
        // Quit when we do not have the entity types info
        final Optional<Set<ApiEntityType>> scopeTypes = scope.getScopeTypes();
        if (!scopeTypes.isPresent()) {
            return;
        }

        // Hosts buying power and cooling is the only case to filter out, so let's only worry about that
        if (scopeTypes.get().stream().noneMatch(SustainabilityCommodityFilterUtils.HOST_AND_DC::contains)) {
            return;
        }

        // We keep a note of the provider ApiIds that we need more information about to determine
        // whether to remove certain power and cooling stats.
        final Set<ApiId> powerCoolingProvidersToCache = removeFakePowerAndCoolingFromDC(stats,
                uuidMapper);

        // If we did not come across any non-cached providers, there is nothing more to do
        if (powerCoolingProvidersToCache.isEmpty()) {
            return;
        }

        // We would need to cache the provider ApiIds that we did not have information of during the
        // first pass. This means that during the second pass (and ever other pass),
        // we would have the information about all the providers of power/cooling in this scope,
        // and this will be available in the cache.
        // Note: This is an expensive task only when we miss the cache on any of the
        // providers of power/cooling commodities in the stats. We limit the number rpc calls made
        // this way.
        uuidMapper.bulkResolveEntities(powerCoolingProvidersToCache);
        SustainabilityCommodityFilterUtils.removeFakePowerAndCoolingFromDC(stats, uuidMapper);
    }

    /**
     * Iterate over the stats and discard bought power/cooling metrics when the provider is a
     * Datacenter entity.
     *
     * @param stats Stat snapshots
     * @param uuidMapper Mapper to convert uuids to oids
     * @return Set of ApiIds of power/cooling providers that we do not have enough information about
     */
    private static Set<ApiId> removeFakePowerAndCoolingFromDC(
            @Nonnull List<StatSnapshotApiDTO> stats, @Nonnull UuidMapper uuidMapper) {
        final Set<ApiId> powerCoolingProvidersToCache = new HashSet<>();
        stats.forEach(statSnapshotApiDTO -> statSnapshotApiDTO.getStatistics().removeIf(statApiDTO -> {
            final List<StatFilterApiDTO> filterApiDTO = statApiDTO.getFilters();
            if (filterApiDTO.isEmpty()) {
                return false;
            }

            // Only focus on bought commodities
            final BaseApiDTO relatedEntity = statApiDTO.getRelatedEntity();
            if (relatedEntity == null || filterApiDTO.stream().noneMatch(
                    statFilterApiDTO -> statFilterApiDTO.getValue()
                            .equals(StringConstants.RELATION_BOUGHT))) {
                return false;
            }

            final CommodityType commodityType = COMMODITY_TYPE_MAPPINGS.get(statApiDTO.getName());
            if (!SustainabilityCommodityFilterUtils.POWER_AND_COOLING.contains(commodityType)) {
                return false;
            }

            // Retrieve the cached provider information.
            final String uuid = relatedEntity.getUuid();
            final ApiId provider;
            try {
                provider = uuidMapper.fromUuid(uuid);
            } catch (Exception e) {
                return false;
            }

            // If we do not find the provider uuid in the cache, we should record this to bulk resolve later
            // do that the subsequent calls are significantly quicker.
            if (!provider.hasCachedEntityInfo()) {
                powerCoolingProvidersToCache.add(provider);
                return false;
            }

            final CachedEntityInfo providerInfo = provider.getCachedEntityInfo().get();

            // Power/Cooling sold by datacenters will be ignored.
            return providerInfo.getEntityType() == ApiEntityType.DATACENTER;
        }));

        return powerCoolingProvidersToCache;
    }
}
