package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Storage for projected reserved instance(RI) coverage of entities. For now we store them in a
 * simple map, because we only need the most recent snapshot and don't need aggregation for it.
 */
@ThreadSafe
public class ProjectedRICoverageAndUtilStore {
    private final Logger logger = LogManager.getLogger();

    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    /**
     * A map with key: VM/DB OID; value: EntityReservedInstanceCoverage.
     */
    private Map<Long, EntityReservedInstanceCoverage> projectedEntitiesRICoverage = new HashMap<>();

    private final RepositoryClient repositoryClient;

    private final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final BuyReservedInstanceStore buyReservedInstanceStore;

    private final Clock clock;

    // This should be the same as realtimeTopologyContextId.
    private long topologyContextId;

    // so we update all the information before any other access to the information
    private final Object lockObject = new Object();

    /**
     * Constructor that takes references to use to get information about requested scope.
     * @param repositoryClient
     *     The repository client to access the scope information
     * @param supplyChainServiceBlockingStub
     *     The supply chain service blocking stub to pass to the scope processing
     * @param reservedInstanceBoughtStore The {@link ReservedInstanceBoughtStore}, used to resolve
     *                                    references to RI utilization
     * @param clock A time source used to define projected stats timestamps
     */
    public ProjectedRICoverageAndUtilStore(
                    @Nonnull RepositoryClient repositoryClient,
                    @Nonnull SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
                    @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                    @Nonnull BuyReservedInstanceStore buyReservedInstanceStore,
                    @Nonnull Clock clock) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.supplyChainServiceBlockingStub =
                        Objects.requireNonNull(supplyChainServiceBlockingStub);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.buyReservedInstanceStore = Objects.requireNonNull(buyReservedInstanceStore);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Update the real time projected entity RI coverage in the store.
     *
     * @param originalTopologyInfo
     *     The information about the topology used to generate the Coverage, currently unused.
     * @param entitiesRICoverage
     *     A stream of the new {@link EntityReservedInstanceCoverage}. These will completely replace
     *     the existing entity RI coverage info.
     */
    public void updateProjectedRICoverage(
                    @Nonnull final TopologyInfo originalTopologyInfo,
                    @Nonnull final List<EntityReservedInstanceCoverage> entitiesRICoverage) {
        synchronized (lockObject) {
            Objects.requireNonNull(originalTopologyInfo, "topology info must not be null");
            topologyContextId = originalTopologyInfo.getTopologyContextId();
            // Clear the coverage map before updating.
            projectedEntitiesRICoverage.clear();
            for (EntityReservedInstanceCoverage entityRICoverage : entitiesRICoverage) {
                projectedEntitiesRICoverage.put(entityRICoverage.getEntityId(), entityRICoverage);
            }
            //TODO: remove this logger or convert to debug
            logger.info("updateProjectedRICoverage topology ID {}, context {} , type {}, size {}",
                            originalTopologyInfo.getTopologyId(),
                            originalTopologyInfo.getTopologyContextId(),
                            originalTopologyInfo.getTopologyType(),
                    projectedEntitiesRICoverage.size());
        }
    }

    /**
     * Get the Reserved Instance Coverage Map, which has VM or DB OID key to value which is a Map of
     * RI ID to Coupons covered by that RI.
     *
     * @return A map with key: VM/DB OID; value: EntityReservedInstanceCoverage.
     */
    @Nonnull
    public Map<Long, EntityReservedInstanceCoverage> getAllProjectedEntitiesRICoverages() {
        synchronized (lockObject) {
            return Collections.unmodifiableMap(projectedEntitiesRICoverage);
        }
    }

    /**
     * Get the Reserved Instance Coverage map for VMs and DBs in the scope defined by the filter. We
     * ask the repository for the collection of EntityTypes and the OIDs of each of those entity
     * types that are in the scope defined by using the filter's set of entity OIDs as the seed.
     *
     * @param filter The information about the scope to use to filter the Coverage Map.
     * @return A map with key: entity_id; value: EntityReservedInstanceCoverage.
     */
    @Nonnull
    public Map<Long, EntityReservedInstanceCoverage>
                    getScopedProjectedEntitiesRICoverages(ReservedInstanceCoverageFilter filter) {
        final Collection<List<Long>> startingOidsPerScope = filter.getStartingOidsPerScope();
        // If this is a global scope, short circuit and avoid the query to the repository
        if (startingOidsPerScope.stream().allMatch(CollectionUtils::isEmpty)) {
            return Collections.unmodifiableMap(projectedEntitiesRICoverage);
        } else {
            final Stream<Map<EntityType, Set<Long>>> entitiesPerScope =
                    repositoryClient.getEntitiesByTypePerScope(startingOidsPerScope,
                    supplyChainServiceBlockingStub);

            final Set<Long> scopedVmOids = entitiesPerScope
                    .map(m -> m.getOrDefault(EntityType.VIRTUAL_MACHINE, Collections.emptySet()))
                    .reduce(Sets::intersection)
                    .map(Collection::stream)
                    .map(stream -> stream.collect(Collectors.toSet()))
                    .orElse(new HashSet<>());

            if (scopedVmOids.isEmpty()) {
                logger.debug("No VMs found in scope. startingOidsPerScope: {}",
                        () -> startingOidsPerScope);
                return Collections.emptyMap();
            }
            final Map<Long, EntityReservedInstanceCoverage> projectedEntityRICoverages;
            synchronized (lockObject) {
                logger.debug("projectedEntitiesRICoverage.size() {}, startingOidsPerScope {}"
                                + ", scopedVmOids.size() {}", projectedEntitiesRICoverage::size,
                        () -> startingOidsPerScope, scopedVmOids::size);
                projectedEntityRICoverages = scopedVmOids.stream()
                        .map(projectedEntitiesRICoverage::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(EntityReservedInstanceCoverage::getEntityId,
                                Function.identity()));

                logger.debug("projectedEntityRICoverages: {}", () -> projectedEntityRICoverages);
            }
            return projectedEntityRICoverages;
        }
    }

    /**
     * Gets the RI coverage for entities, filtered through {@code filter}, represented as a single
     * {@link ReservedInstanceStatsRecord}. The timestamp of the stats record will be the
     * current time + {@link #PROJECTED_STATS_TIME_IN_FUTURE_HOURS}.
     *
     * @param filter The filter, applied to entities within the projected {@link EntityReservedInstanceCoverage}
     *               data.
     * @param includeBuyRICoverage A flag indicating whether coverage from Buy RI instances should be
     *                             included in the returned stats record.
     * @param endDate                 time to be used for projectedStats.
     * @return A {@link ReservedInstanceStatsRecord}, in which capacity represents the coverage capacity
     * of entities within scope and the value of the record represented the covered amount (assumed
     * to be in coupons) of the entities within scope.
     */
    @Nonnull
    public ReservedInstanceStatsRecord getReservedInstanceCoverageStats(
            @Nonnull ReservedInstanceCoverageFilter filter,
            boolean includeBuyRICoverage, final long endDate) {

        final Map<Long, EntityReservedInstanceCoverage> projectedEntitiesRICoverages =
                getScopedProjectedEntitiesRICoverages(filter);
        double usedCouponsTotal = 0d;
        double entityCouponsCapTotal = 0d;
        for (EntityReservedInstanceCoverage entityRICoverage : projectedEntitiesRICoverages.values()) {

            // Add coverage from RI inventory
            usedCouponsTotal += entityRICoverage.getCouponsCoveredByRiMap()
                    .values()
                    .stream()
                    .mapToDouble(Double::doubleValue)
                    .sum();

            // Add coverage from Buy RI actions, if requested
            if (includeBuyRICoverage) {
                usedCouponsTotal += entityRICoverage.getCouponsCoveredByBuyRiMap()
                        .values()
                        .stream()
                        .mapToDouble(Double::doubleValue)
                        .sum();
            }


            entityCouponsCapTotal += entityRICoverage.getEntityCouponCapacity();
        }
        final long projectedTime = endDate != 0 ? endDate : clock.instant()
                .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
        return ReservedInstanceUtil.createRIStatsRecord((float)entityCouponsCapTotal,
                (float)usedCouponsTotal, projectedTime);
    }

    /**
     * Calculates and returns RI utilization as a {@link ReservedInstanceStatsRecord}, with RIs in scope
     * determined by {@code filter}. RIs in scope are determined by converting the
     * {@link ReservedInstanceUtilizationFilter} to a {@link ReservedInstanceBoughtFilter} and querying
     * the {@link ReservedInstanceBoughtStore}.
     *
     * @param filter                  The {@link ReservedInstanceUtilizationFilter} instance, assumed to be applied to
     *                                all utilization records for a larger stats request.
     * @param includeBuyRIUtilization Indicates whether utilization of Buy RI instances should be included
     *                                in the returned stats record.
     * @param endDate                 time to be used for projectedStats.
     * @return An instance of {@link ReservedInstanceStatsRecord}, in which the capacity is the coupon
     * capacity of all RIs in scope and the value is the coverage amount used for all RIs in scope.
     */
    @Nonnull
    public ReservedInstanceStatsRecord getReservedInstanceUtilizationStats(
            @Nonnull ReservedInstanceUtilizationFilter filter,
            boolean includeBuyRIUtilization, long endDate) {

        // First, query the RI bought store to determine the RIs in scope. The full ReservedInstanceBoughtInfo
        // is queried, in order to determine the RI capacity as well.
        final ReservedInstanceBoughtFilter riBoughtFilter = filter.toReservedInstanceBoughtFilter();
        final List<ReservedInstanceBought> risInScope =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(riBoughtFilter);
        final Set<Long> riBoughtIdsInScope = risInScope.stream()
                .map(ReservedInstanceBought::getId)
                .collect(ImmutableSet.toImmutableSet());

        // Resolve Buy RI instances in scope
        final List<ReservedInstanceBought> buyRIsInScope = includeBuyRIUtilization ?
                resolveBuyRIsInScope(filter) : Collections.emptyList();
        final Set<Long> buyRIIdsInScope = buyRIsInScope.stream()
                .map(ReservedInstanceBought::getId)
                .collect(ImmutableSet.toImmutableSet());

        final long projectedTime = endDate != 0 ? endDate : clock.instant()
                .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();

        // Determine the capacity of both RI inventory and buy RI instances.
        final long coverageCapacity = Stream.of(risInScope, buyRIsInScope)
                .flatMap(List::stream)
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceBoughtCoupons)
                .mapToLong(ReservedInstanceBoughtCoupons::getNumberOfCoupons)
                .sum();

        synchronized (lockObject) {
            final double riInventoryCoverageUtilization =
                    riBoughtIdsInScope.isEmpty() ? 0.0 :
                            projectedEntitiesRICoverage.values()
                                    .stream()
                                    .map(EntityReservedInstanceCoverage::getCouponsCoveredByRiMap)
                                    .map(Map::entrySet)
                                    .flatMap(Set::stream)
                                    .filter(riEntry -> riBoughtIdsInScope.contains(riEntry.getKey()))
                                    .mapToDouble(Entry::getValue)
                                    .sum();

            // Look through the EntityReservedInstanceCoverage::getCouponsCoveredByBuyRiMap, containing
            // RI coverage from buy RI instance from BuyRIImpactAnalysis
            final double buyRICoverageUtilization =
                    buyRIIdsInScope.isEmpty() ? 0.0 :
                            projectedEntitiesRICoverage.values()
                                    .stream()
                                    .map(EntityReservedInstanceCoverage::getCouponsCoveredByBuyRiMap)
                                    .map(Map::entrySet)
                                    .flatMap(Set::stream)
                                    .filter(riEntry -> buyRIIdsInScope.contains(riEntry.getKey()))
                                    .mapToDouble(Entry::getValue)
                                    .sum();

            final double totalCoverageUtilization = riInventoryCoverageUtilization + buyRICoverageUtilization;

            return ReservedInstanceUtil.createRIStatsRecord((float)coverageCapacity,
                    (float)totalCoverageUtilization, projectedTime);

        }
    }

    /**
     * Determines the RIs in scope of the {@code riUitlizationFilter}. First, the filter is converted
     * to a {@link BuyReservedInstanceFilter}. The {@link BuyReservedInstanceStore} is queried with
     * the converted filter, using the realtime topology context ID
     *
     * @param riUtilizationFilter The source filter to scope BuyRI instances
     * @return A list of BuyRI instances (represented by {@link ReservedInstanceBought}) in scope
     */
    private List<ReservedInstanceBought> resolveBuyRIsInScope(
            @Nonnull ReservedInstanceUtilizationFilter riUtilizationFilter) {

        // The API should not request buy RI utilization in zone scopes. Therefore, if this is a zone
        // scope and buy RI utilization is requested, throw an unsupported operation exception to
        // indicate an invalid request.
        if (riUtilizationFilter.isZoneFiltered()) {
            throw new UnsupportedOperationException(
                    "Zone filtering is not compatible with Buy RI utilization");
        } else {
            final BuyReservedInstanceFilter buyReservedInstanceFilter = BuyReservedInstanceFilter.newBuilder()
                    .addTopologyContextId(topologyContextId)
                    .setAccountFilter(riUtilizationFilter.getAccountFilter())
                    .setRegionFilter(riUtilizationFilter.getRegionFilter())
                    .build();

            return buyReservedInstanceStore.getBuyReservedInstances(buyReservedInstanceFilter);
        }
    }
}
