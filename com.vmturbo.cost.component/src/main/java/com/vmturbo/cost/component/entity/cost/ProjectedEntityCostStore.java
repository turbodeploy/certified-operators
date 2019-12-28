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
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.Builder;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
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
public class ProjectedEntityCostStore {

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

    private final Logger logger = LogManager.getLogger();


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
            final Set<Long> entityScopedOids = filter.getEntityFilters().orElse(null);
            final Set<Long> regionScopedEntityOids = filter.getRegionIds()
                    .map(this::getEntityOidsFromRepository).orElse(null);
            final Set<Long> zoneScopedEntityOids = filter.getAvailabilityZoneIds()
                    .map(this::getEntityOidsFromRepository).orElse(null);
            final Set<Long> accountScopedEntityOids = filter.getAccountIds()
                    .map(this::getEntityOidsFromRepository).orElse(null);

            final Set<Long> entitiesToOver = Stream.of(entityScopedOids, regionScopedEntityOids,
                    zoneScopedEntityOids, accountScopedEntityOids)
                    .filter(Objects::nonNull)
                    .reduce(Sets::intersection)
                    .map(Collection::stream)
                    .map(oids -> oids.collect(Collectors.toSet()))
                    .orElseGet(this::getAllStoredProjectedEntityOids);

            logger.trace("Entities in scope for projected entities costs: {}",
                    () -> entitiesToOver);
            // apply the filters and return
            return entitiesToOver.stream()
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

    private Set<Long> getEntityOidsFromRepository(final Set<Long> scopeIds) {
        return scopeIds.isEmpty() ? new HashSet<>()
                : repositoryClient.getEntityOidsByType(new ArrayList<>(scopeIds),
                realTimeTopologyContextId,
                supplyChainServiceBlockingStub).values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
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

    private Optional<EntityCost> applyFilter(EntityCost entityCost, EntityCostFilter filter) {
        // If entity in question is not any of the requested types ignore it
        if (filter.getEntityTypeFilters().isPresent()
                && !filter.getEntityTypeFilters().get().contains(entityCost.getAssociatedEntityType())) {
            return Optional.empty();
        }

        EntityCost.Builder builder = entityCost.toBuilder();

        List<EntityCost.ComponentCost> filteredComponentCosts = entityCost.getComponentCostList()
            .stream()
            .filter(filter::filterComponentCost)
            .collect(Collectors.toList());

        if (filteredComponentCosts.isEmpty()) {
            return Optional.empty();
        }

        builder.clearComponentCost()
            .addAllComponentCost(filteredComponentCosts);

        return Optional.of(builder.build());
    }

    /**
     * Helper method to hold collection of aggregated stat record.
     *
     * @return List of {@link StatRecord}.
     */
    @Nonnull
    @VisibleForTesting
    public Collection<StatRecord> getProjectedStatRecordsByGroup(final List<GroupBy> groupByList,
                                                                 @Nonnull final EntityCostFilter entityCostFilter) {
        Preconditions.checkArgument(entityCostFilter.getCostGroupBy() != null &&
                !entityCostFilter.getCostGroupBy().getGroupByFields().isEmpty(), "GroupBy list should not be empty.");
        Set<Integer> costCategoryFilter = new HashSet<>();
        if (entityCostFilter.getCostCategoryFilter().isPresent()) {
            costCategoryFilter = entityCostFilter.getCostCategoryFilter().get()
                    .getCostCategoryList().stream().map(CostCategory::getNumber)
                    .collect(Collectors.toSet());
        }
        Map<Long, EntityCost> costSnapshot = getProjectedEntityCosts(entityCostFilter);
        Collection<StatRecord>  result = Lists.newArrayList();
        if (costCategoryFilter.size() > 2 ) {
            // because groupBy > 2 is actually all the rows.
            return getProjectedStatRecords(entityCostFilter);
        } else {
            for (final EntityCost value : costSnapshot.values()) {
                result.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(value));
            }
            result = aggregateByGroup(groupByList, result);
        }
        return result;
    }

    /**
     * Returned aggregated/merged {@link StatRecord} based on params.
     *
     * @param groupBys List of properties to groupBy.
     * @param statRecords list of {@link StatRecord} to merge.
     * @return List of merged {@link StatRecord}.
     */
    @Nonnull
    @VisibleForTesting
    public Collection<StatRecord> aggregateByGroup(@Nonnull final Collection<GroupBy> groupBys,
                                                    @Nonnull final Collection<StatRecord> statRecords) {
        //creating a map indexed by group propertyNames.
        final Map<String, StatRecord> groupedStatRecord = Maps.newHashMap();
        statRecords.forEach(item -> {
            List<String> keys = new ArrayList<>();
            if (groupBys.contains(GroupBy.COST_CATEGORY)) {
                keys .add(item.getCategory().name());
            }
            if (groupBys.contains(GroupBy.ENTITY_TYPE)) {
                keys.add(String.valueOf(item.getAssociatedEntityType()));
            }
            if (groupBys.contains(GroupBy.ENTITY)) {
                keys.add(String.valueOf(item.getAssociatedEntityId() ));
            }
            groupedStatRecord.compute(Strings.join(keys, '-'), (currentKey, currentVal) -> mergeStats(item, currentVal));
        });
        return groupedStatRecord.values();
    }

    /**
     * Merge 2 StatRecords values and other Arrtibs into one.
     *
     * @param newIncomingValue new StatRecord.
     * @param currentVal       current StatRecord.
     * @return Merged StatRecord with aggregated values.
     */
    @Nonnull
    @VisibleForTesting
    public StatRecord mergeStats(@Nonnull final StatRecord newIncomingValue,
                                  @Nullable final StatRecord currentVal) {
        if (currentVal == null) {
            return newIncomingValue;
        }
        final Builder currentBuilder = currentVal.toBuilder();
        boolean allAttirbuteMismatch = true;

        if (newIncomingValue.getAssociatedEntityId() != currentVal.getAssociatedEntityId()) {
            currentBuilder.clearAssociatedEntityId();
            allAttirbuteMismatch = false;
        }
        if (newIncomingValue.getAssociatedEntityType() != currentVal.getAssociatedEntityType()) {
            currentBuilder.clearAssociatedEntityType();
            allAttirbuteMismatch = false;
        }
        if (!newIncomingValue.getCategory().equals(currentVal.getCategory())) {
            currentBuilder.clearCategory();
            allAttirbuteMismatch = false;
        }
        Preconditions.checkArgument(!allAttirbuteMismatch, "None of the common fields matched." +
                "Can not merge cost entities.");
        return currentBuilder.setValues(StatValue.newBuilder()
                .setTotal(newIncomingValue.getValues().getTotal() + currentVal.getValues().getTotal())
                .setMax(Math.max(newIncomingValue.getValues().getMax(), currentVal.getValues().getMax()))
                .setMin(Math.min(newIncomingValue.getValues().getMin(), currentVal.getValues().getMin()))
                .setAvg((newIncomingValue.getValues().getAvg() + currentVal.getValues().getAvg()) / 2)
                .build())
            .build();
    }
}
