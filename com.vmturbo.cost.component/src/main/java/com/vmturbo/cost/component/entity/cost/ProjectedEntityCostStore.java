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
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Storage for projected per-entity costs.
 *
 * For now we store them in a simple map, because we only need the most recent snapshot and don't
 * need aggregation for it.
 */
@ThreadSafe
public class ProjectedEntityCostStore extends AbstractProjectedEntityCostStore {

    /**
     * A map of (oid) -> (most projected entity cost for the oid).
     *
     * This map should be updated atomically. All interactions with it should by synchronized
     * on the lock.
     */
    @GuardedBy("entityCostMapLock")
    private Map<Long, EntityCost> projectedEntityCostByEntity = Collections.emptyMap();

    private final Object entityCostMapLock = new Object();

    private final SupplyChainServiceBlockingStub supplyChainServiceBlockingStub;

    private final long realTimeTopologyContextId;

    private final RepositoryClient repositoryClient;

    private boolean storeReady = false;

    public ProjectedEntityCostStore(@Nonnull RepositoryClient repositoryClient, @Nonnull
            SupplyChainServiceBlockingStub supplyChainServiceBlockingStub,
                             long realTimeTopologyContextId) {
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.supplyChainServiceBlockingStub =
                Objects.requireNonNull(supplyChainServiceBlockingStub);
        this.realTimeTopologyContextId = realTimeTopologyContextId;
    }


    /**
     * Update the projected entity costs in the store.
     *
     * @param entityCosts A list of the new {@link EntityCost}. These will completely replace
     *                    the existing entity costs.
     */
    public void updateProjectedEntityCosts(@Nonnull final List<EntityCost> entityCosts) {
        final Map<Long, EntityCost> newCostsByEntity =
            entityCosts.stream().collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
        synchronized (entityCostMapLock) {
            projectedEntityCostByEntity = Collections.unmodifiableMap(newCostsByEntity);
        }
        storeReady = true;
    }

    @Nonnull
    public Map<Long, EntityCost> getAllProjectedEntitiesCosts() {
        return projectedEntityCostByEntity;
    }

    @Nonnull
    public Collection<StatRecord> getProjectedStatRecords(@Nonnull final EntityCostFilter filter) {
        return EntityCostToStatRecordConverter.convertEntityToStatRecord(getProjectedEntityCosts(filter).values());
    }

    /**
     * Get the projected entity costs for a set of entities.
     *
     * @param entityIds The entities to retrieve the costs for. An empty set will get no results.
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     * associated projected costs will not have an entry in the map.
     */
    @Nonnull
    public Map<Long, EntityCost> getProjectedEntityCosts(@Nonnull final Set<Long> entityIds) {
        if (entityIds.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Long, EntityCost> costSnapshot;
        synchronized (entityCostMapLock) {
            costSnapshot = projectedEntityCostByEntity;
        }

        return getProjectedEntityCostsByEntityId(costSnapshot, entityIds);
    }

    /**
     * Gets the projected cost for entities based on the input filter.
     *
     * @param filter the input filter.
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     *         associated projected costs after filters will not have an entry in the map.
     */
    @Nonnull
    @VisibleForTesting
    public Map<Long, EntityCost> getProjectedEntityCosts(@Nonnull final EntityCostFilter filter) {
        if (filter.isGlobalScope()) {
            return getAllProjectedEntitiesCosts();
        } else {
            final Collection<List<Long>> startingOidsPerScope = Stream.of(filter.getEntityFilters(),
                    filter.getRegionIds(), filter.getAvailabilityZoneIds(), filter.getAccountIds())
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(ArrayList::new)
                    .collect(Collectors.toSet());

            final Set<Long> entitiesInScope;

            if (startingOidsPerScope.stream().allMatch(CollectionUtils::isEmpty)) {
                entitiesInScope = getAllStoredProjectedEntityOids();
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
                        .orElse(getAllStoredProjectedEntityOids());

            }
            getLogger().trace("Entities in scope for projected entities costs: {}",
                    () -> entitiesInScope);
            // apply the filters and return
            return entitiesInScope.stream()
                .map(projectedEntityCostByEntity::get)
                .filter(Objects::nonNull)
                .map(entityCost -> applyFilter(entityCost, filter))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
        }
    }

    private Set<Long> getAllStoredProjectedEntityOids() {
        final Set<Long> entityOids;
        synchronized (entityCostMapLock) {
            entityOids = new HashSet<>(projectedEntityCostByEntity.keySet());
        }
        return entityOids;
    }

    private Map<Long, EntityCost> getProjectedEntityCostsByEntityId(final Map<Long, EntityCost> costSnapshot,
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
    public Collection<StatRecord> getProjectedStatRecordsByGroup(@Nonnull final List<GroupBy> groupByList,
                                                                 @Nonnull final EntityCostFilter entityCostFilter) {
        Preconditions.checkArgument(entityCostFilter.getCostGroupBy() != null &&
                !entityCostFilter.getCostGroupBy().getGroupByFields().isEmpty(), "GroupBy list should not be empty.");
        Map<Long, EntityCost> costSnapshot = getProjectedEntityCosts(entityCostFilter);
        Collection<StatRecord>  result = Lists.newArrayList();
        if (groupByList.size() > 2 ) {
            // because groupBy > 2 is actually all the rows.
            return EntityCostToStatRecordConverter.convertEntityToStatRecord(costSnapshot.values());
        } else {
            for (final EntityCost value : costSnapshot.values()) {
                result.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(value));
            }
            result = aggregateByGroup(groupByList, result);
        }
        return result;
    }

    /**
     * Returns when we see an update to {@link #projectedEntityCostByEntity}.
     *
     * @return true if store ready.
     */
    public boolean isStoreReady() {
        return storeReady;
    }
}
