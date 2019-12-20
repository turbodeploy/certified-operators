package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceFilter;
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
                    @Nonnull Clock clock) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.supplyChainServiceBlockingStub =
                        Objects.requireNonNull(supplyChainServiceBlockingStub);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
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
     * @return A map with key: RI_ID; value: EntityReservedInstanceCoverage.
     */
    @Nonnull
    public Map<Long, EntityReservedInstanceCoverage>
                    getScopedProjectedEntitiesRICoverages(ReservedInstanceCoverageFilter filter) {
        // Do the RPC before getting the lock, since the RPC can take a long time.
        final List<Long> scopeIds = filter.getScopeOids();

        // If this is a global scope, shortcircuit and avoid the query to the repository
        if (scopeIds.isEmpty()) {
            return Collections.unmodifiableMap(projectedEntitiesRICoverage);
        } else {
            // getEntityOidsByType gets all entities in the real time topology if scopeIds is empty.
            Map<EntityType, Set<Long>> scopeMap = repositoryClient.getEntityOidsByType(scopeIds,
                    topologyContextId, supplyChainServiceBlockingStub);
            // this may return null if there are no VMs in scope, check below.
            Set<Long> scopedOids = scopeMap.getOrDefault(EntityType.VIRTUAL_MACHINE, Collections.emptySet());
            //TODO: add support for database VMs, make sure DATABASE is correct EntityType for them
            //scopedOids.addAll(scopeMap.get(EntityType.DATABASE));

            synchronized (lockObject) {
                if (scopedOids == null) {
                    logger.debug("projectedEntitiesRICoverage.size() {}, scopeIds.size() {}"
                                    + ", no entities found in scope",
                            projectedEntitiesRICoverage::size, scopeIds::size);
                    return Collections.EMPTY_MAP;
                }
                Map<Long, EntityReservedInstanceCoverage> filteredMap = new HashMap<>();
                logger.debug("projectedEntitiesRICoverage.size() {}, scopeIds.size() {}"
                                + ", scopedOids.size() {}", projectedEntitiesRICoverage::size,
                        scopeIds::size, scopedOids::size);
                for (Long anOid : scopedOids) {
                    EntityReservedInstanceCoverage value = projectedEntitiesRICoverage.get(anOid);
                    if (value != null) {
                        filteredMap.put(anOid, value);
                        logger.info("For VM OID {} found projected coverage {}", anOid, value);
                    } else {
                        logger.info("For VM OID {} no projected coverage found", anOid);
                    }
                }
                return Collections.unmodifiableMap(filteredMap);
            }
        }
    }

    /**
     * Gets the RI coverage for entities, filtered through {@code filter}, represented as a single
     * {@link ReservedInstanceStatsRecord}. The timestamp of the stats record will be the
     * current time + {@link #PROJECTED_STATS_TIME_IN_FUTURE_HOURS}.
     *
     * @param filter The filter, applied to entities within the projected {@link EntityReservedInstanceCoverage}
     *               data.
     * @return A {@link ReservedInstanceStatsRecord}, in which capacity represents the coverage capacity
     * of entities within scope and the value of the record represented the covered amount (assumed
     * to be in coupons) of the entities within scope.
     */
    @Nonnull
    public ReservedInstanceStatsRecord getReservedInstanceCoverageStats(
            @Nonnull ReservedInstanceCoverageFilter filter) {

        final Map<Long, EntityReservedInstanceCoverage> projectedEntitiesRICoverages =
                getScopedProjectedEntitiesRICoverages(filter);
        double usedCouponsTotal = 0d;
        double entityCouponsCapTotal = 0d;
        for (EntityReservedInstanceCoverage entityRICoverage : projectedEntitiesRICoverages.values()) {
            final Map<Long, Double> riMap = entityRICoverage.getCouponsCoveredByRiMap();
            if (riMap != null) {
                usedCouponsTotal += riMap.values().stream().mapToDouble(Double::doubleValue).sum();
            }
            entityCouponsCapTotal += entityRICoverage.getEntityCouponCapacity();
        }
        final long projectedTime = clock.instant()
                .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
        return ReservedInstanceUtil.createRIStatsRecord((float)entityCouponsCapTotal,
                (float)usedCouponsTotal, projectedTime);
    }

    /**
     * Calculates and returns RI utilization as a {@link ReservedInstanceStatsRecord}, with RIs in scope
     * determined by {@code filter}. RIs in scope are determined by converting the
     * {@link ReservedInstanceUtilizationFilter} to a {@link ReservedInstanceBoughtFilter} and querying
     * the {@link ReservedInstanceBoughtStore}.
     * @param filter The {@link ReservedInstanceUtilizationFilter} instance, assumed to be applied to
     *               all utilization records for a larger stats request.
     * @return An instance of {@link ReservedInstanceStatsRecord}, in which the capacity is the coupon
     * capacity of all RIs in scope and the value is the coverage amount used for all RIs in scope.
     */
    @Nonnull
    public ReservedInstanceStatsRecord getReservedInstanceUtilizationStats(
            @Nonnull ReservedInstanceUtilizationFilter filter) {

        // FIrst, query the RI bought store to determine the RIs in scope. The full ReservedInstanceBoughtInfo
        // is queried, in order to determine the RI capacity as well.
        final ReservedInstanceBoughtFilter riBoughtFilter = filter.toReservedInstanceBoughtFilter();
        final List<ReservedInstanceBought> risInScope =
                reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(riBoughtFilter);
        final Set<Long> riBoughtIdsInScope = risInScope.stream()
                .map(ReservedInstanceBought::getId)
                .collect(ImmutableSet.toImmutableSet());

        final long projectedTime = clock.instant()
                .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();

        final long coverageCapacity = risInScope.stream()
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceBoughtCoupons)
                .mapToLong(ReservedInstanceBoughtCoupons::getNumberOfCoupons)
                .sum();

        synchronized (lockObject) {
            final double coverageUtilization = projectedEntitiesRICoverage.values()
                    .stream()
                    .map(EntityReservedInstanceCoverage::getCouponsCoveredByRiMap)
                    .map(Map::entrySet)
                    .flatMap(Set::stream)
                    .filter(riEntry -> riBoughtIdsInScope.contains(riEntry.getKey()))
                    .mapToDouble(Entry::getValue)
                    .sum();

            return ReservedInstanceUtil.createRIStatsRecord((float)coverageCapacity,
                    (float)coverageUtilization, projectedTime);

        }
    }
}
