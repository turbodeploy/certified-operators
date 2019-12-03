package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.util.Strings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.Builder;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;

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

    /**
     * Get the projected entity costs for a set of entities.
     *
     * @param entityIds The entities to retrieve the costs for. An empty set will get no results.
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     * associated projected costs will not have an entry in the map.
     */
    @Nonnull
    public Map<Long, EntityCost> getProjectedEntityCosts(@Nonnull final Set<Long> entityIds) {
        final Map<Long, EntityCost> costSnapshot;
        synchronized (entityCostMapLock) {
            costSnapshot = projectedEntityCostByEntity;
        }

        return entityIds.stream()
                .map(costSnapshot::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
    }

    /**
     * Get the projected entity costs for a set of entities.
     *
     * @param entityIds The entities to retrieve the costs for. An empty set will get no results.
     * @return collection of StatRecord.
     */
    @Nonnull
    public Collection<StatRecord> getProjectedStatRecords(@Nonnull final Set<Long> entityIds) {
        final Map<Long, EntityCost> costSnapshot;
        synchronized (entityCostMapLock) {
            costSnapshot = projectedEntityCostByEntity;
        }
        return convertEntityCostToStatRecord(entityIds, costSnapshot);
    }

    /**
     * Convert Map of id - > EntityCost to StatRecords.
     * @param entityIds filtered entityIds.
     * @param costSnapshot map of id - > EntityCost.
     * @return List of StatRecords.
     */
    @Nonnull
    @VisibleForTesting
    public  Collection<StatRecord>  convertEntityCostToStatRecord(
            @Nonnull final Set<Long> entityIds,
            @Nonnull final Map<Long, EntityCost> costSnapshot) {
        final Map<Long, EntityCost> entityCostMap = entityIds.stream()
            .map(costSnapshot::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
        // populate cost per entity, and up to the caller to aggregateByGroup as needed.
        final Map<Long, Collection<StatRecord>> statRecordsByTimeStamp = Maps.newHashMap();

        entityCostMap.forEach((time, entityCost) -> {
            statRecordsByTimeStamp.compute(time, (key, currentValue) -> {
                if (currentValue == null) {
                    currentValue = new ArrayList<>();
                }
                currentValue.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCost));
                return currentValue;
            });
        });
        return statRecordsByTimeStamp.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * Helper method to hold collection of aggregated stat record.
     *
     * @param groupBySet List of properties to groupBy.
     * @return List of {@link StatRecord}.
     */
    @Nonnull
    @VisibleForTesting
    public Collection<StatRecord> getProjectedStatRecordsByGroup(@Nonnull final List<GroupBy> groupBySet) {
        Preconditions.checkArgument(!groupBySet.isEmpty(), "GroupBy list should not be empty.");
        final Map<Long, EntityCost> costSnapshot;
        synchronized (entityCostMapLock) {
            costSnapshot = projectedEntityCostByEntity;
        }
        Collection<StatRecord>  result = Lists.newArrayList();
        if (groupBySet.size() > 2 ) {
            // because groupBy > 2 is actually all the rows.
            return getProjectedStatRecords(Collections.emptySet());
        } else {
            for (final EntityCost value : costSnapshot.values()) {
                result.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(value));
            }
            result = aggregateByGroup(groupBySet, result);
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
