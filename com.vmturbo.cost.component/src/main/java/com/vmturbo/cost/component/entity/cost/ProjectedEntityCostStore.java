package com.vmturbo.cost.component.entity.cost;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.cost.component.util.EntityCostFilter;

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

    private final int chunkSize = 1000;

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

    /**
     * Get the projected entity costs for a set of entities.
     *
     * @param entityIds The entities to retrieve the costs for. An empty set will get no results.
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     *         associated projected costs will not have an entry in the map.
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
     * Gets the projected cost for entities based on the input filter.
     * @param filter the input filter.
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     *         associated projected costs after filters will not have an entry in the map.
     */
    @Nonnull
    public Map<Long, EntityCost> getProjectedEntityCosts(@Nonnull final EntityCostFilter filter) {
        if (!filter.getEntityTypeFilters().isPresent()
            && !filter.getEntityFilters().isPresent()
            && !filter.getCostCategoryFilter().isPresent()
            && !filter.getCostSources().isPresent()) {
            return getAllProjectedEntitiesCosts();
        } else {
            Set<Long> entitiesToOver;
            if (filter.getEntityFilters().isPresent()) {
                entitiesToOver = filter.getEntityFilters().get();
            } else {
                // Create a copy of all entities in the map keyset
                synchronized (entityCostMapLock) {
                    entitiesToOver = new HashSet<>(projectedEntityCostByEntity.keySet());
                }
            }

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


    @Nonnull
    public Map<Long, EntityCost> getAllProjectedEntitiesCosts() {
        return projectedEntityCostByEntity;
    }

}
