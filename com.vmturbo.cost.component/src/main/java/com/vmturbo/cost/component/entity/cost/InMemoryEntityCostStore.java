package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Storage for in-memory per-entity costs.
 * For now we store them in a simple map, because we only need the most recent snapshot and don't
 * need aggregation for it.
 */
@ThreadSafe
public class InMemoryEntityCostStore extends AbstractProjectedEntityCostStore {

    /**
     * A map of (oid) -> (entity cost for the oid).
     * This map should be updated atomically. All interactions with it should by synchronized
     * on the lock.
     */
    @GuardedBy("entityCostMapLock")
    private Map<Long, EntityCost> entityCostByEntity = Collections.emptyMap();

    private final Object entityCostMapLock = new Object();

    private final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;


    private final long realTimeTopologyContextId;

    private final RepositoryClient repositoryClient;

    private boolean storeReady = false;

    /**
     * Constructor.
     *
     * @param repositoryClient the repository client
     * @param supplyChainServiceBlockingStub supply chai service client
     * @param realTimeTopologyContextId real time context id
     */
    public InMemoryEntityCostStore(@Nonnull RepositoryClient repositoryClient, @Nonnull
            SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
                                   long realTimeTopologyContextId) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.supplyChainServiceBlockingStub =
                Objects.requireNonNull(supplyChainServiceBlockingStub);
        this.realTimeTopologyContextId = realTimeTopologyContextId;
    }


    /**
     * Update the entity costs in the store.
     *
     * @param entityCosts A list of the new {@link EntityCost}. These will completely replace
     *                    the existing entity costs.
     */
    public void updateEntityCosts(@Nonnull final List<EntityCost> entityCosts) {
        final Map<Long, EntityCost> newCostsByEntity =
            entityCosts.stream().collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
        synchronized (entityCostMapLock) {
            entityCostByEntity = Collections.unmodifiableMap(newCostsByEntity);
        }
        storeReady = true;
    }

    /**
     * Return the cached entity costs.
     *
     * @return entity costs.
     */
    @Nonnull
    public Map<Long, EntityCost> getEntitiesCosts() {
        return entityCostByEntity;
    }

    /**
     * Return entity cost stat records for a given cost filter.
     *
     * @param filter entity cst filter.
     * @return entity  stat records.
     */
    @Nonnull
    public Collection<StatRecord> getEntityCostStatRecords(@Nonnull final EntityCostFilter filter) {
        final TimeFrame timeFrame =
                filter.isTotalValuesRequested() ? filter.getTimeFrame() : TimeFrame.HOUR;
        return EntityCostToStatRecordConverter.convertEntityToStatRecord(
                getEntityCosts(filter).values(), timeFrame);
    }

    /**
     * Get the cached entity costs for a set of entities.
     *
     * @param entityIds The entities to retrieve the costs for. An empty set will get no results.
     * @return A map of (id) -> (entity cost). Entities in the input that do not have an
     * associated costs will not have an entry in the map.
     */
    @Nonnull
    public Map<Long, EntityCost> getEntityCosts(@Nonnull final Set<Long> entityIds) {
        if (entityIds.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Long, EntityCost> costSnapshot;
        synchronized (entityCostMapLock) {
            costSnapshot = entityCostByEntity;
        }

        return getInMemoryEntityCostsByEntityId(costSnapshot, entityIds);
    }

    /**
     * Gets the cached entity costs for entities based on the input filter.
     *
     * @param filter the input filter.
     * @return A map of (id) -> (entity cost). Entities in the input that do not have an
     *         associated costs after filters will not have an entry in the map.
     */
    @Nonnull
    @VisibleForTesting
    public Map<Long, EntityCost> getEntityCosts(@Nonnull final EntityCostFilter filter) {
        if (filter.isGlobalScope()) {
            return getEntitiesCosts();
        } else {
            final Collection<List<Long>> startingOidsPerScope = Stream.of(filter.getEntityFilters(),
                    filter.getRegionIds(), filter.getAvailabilityZoneIds(), filter.getAccountIds())
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(ArrayList::new)
                    .collect(Collectors.toSet());

            final Set<Long> entitiesInScope;

            if (startingOidsPerScope.stream().allMatch(CollectionUtils::isEmpty)) {
                entitiesInScope = getAllStoredInMemoryCostEntityOids();
            } else {
                final Stream<Set<Long>> entitiesPerScope =
                        repositoryClient.getEntitiesByTypePerScope(startingOidsPerScope,
                                supplyChainServiceBlockingStub)
                                .map(m -> m.values().stream()
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toSet()));
                entitiesInScope = entitiesPerScope
                        .reduce(Sets::intersection)
                        .map(Collection::stream)
                        .map(oids -> oids.collect(Collectors.toSet()))
                        .orElse(getAllStoredInMemoryCostEntityOids());

            }
            getLogger().trace("Entities in scope for cached entities costs: {}",
                    () -> entitiesInScope);
            // apply the filters and return
            return entitiesInScope.stream()
                .map(entityCostByEntity::get)
                .filter(Objects::nonNull)
                .map(entityCost -> applyFilter(entityCost, filter))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
        }
    }

    private Set<Long> getAllStoredInMemoryCostEntityOids() {
        final Set<Long> entityOids;
        synchronized (entityCostMapLock) {
            entityOids = new HashSet<>(entityCostByEntity.keySet());
        }
        return entityOids;
    }

    private Map<Long, EntityCost> getInMemoryEntityCostsByEntityId(final Map<Long, EntityCost> costSnapshot,
                                                                   @Nonnull final Set<Long> entityIds) {
        if (entityIds.isEmpty()) {
            return costSnapshot;
        } else {
            Map<Long, EntityCost> entityCostHashMap = new HashMap<>();
            costSnapshot.entrySet().stream()
                    .filter(i1 -> entityIds.contains(i1.getKey()))
                    .forEach(entry -> {
                        entityCostHashMap.put(entry.getKey(), entry.getValue());
                    });
            return entityCostHashMap;
        }
    }

    /**
     * Helper method to hold collection of aggregated stat record.
     * @param groupByList list of groupBy criteria.
     * @param entityCostFilter entity filter to be used for aggregation.
     * @return List of {@link StatRecord}.
     */
    @Nonnull
    @VisibleForTesting
    public Collection<StatRecord> getEntityCostStatRecordsByGroup(@Nonnull final List<GroupBy> groupByList,
                                                                  @Nonnull final EntityCostFilter entityCostFilter) {
        Preconditions.checkArgument(entityCostFilter.getCostGroupBy() != null
                && !entityCostFilter.getCostGroupBy().getGroupByFields().isEmpty(), "GroupBy list should not be empty.");
        Map<Long, EntityCost> costSnapshot = getEntityCosts(entityCostFilter);
        Collection<StatRecord>  result = Lists.newArrayList();
        final TimeFrame timeFrame =
                entityCostFilter.isTotalValuesRequested() ? entityCostFilter.getTimeFrame()
                        : TimeFrame.HOUR;
        if (groupByList.size() > 2) {
            // because groupBy > 2 is actually all the rows.
            return EntityCostToStatRecordConverter.convertEntityToStatRecord(costSnapshot.values(),
                    timeFrame);
        } else {
            for (final EntityCost value : costSnapshot.values()) {
                result.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(value,
                        timeFrame));
            }
            result = aggregateByGroup(groupByList, result);
        }
        return result;
    }

    /**
     * Returns when we see an update to {@link #entityCostByEntity}.
     *
     * @return true if store ready.
     */
    public boolean isStoreReady() {
        return storeReady;
    }

    public long getRealTimeTopologyContextId() {
        return realTimeTopologyContextId;
    }
}
