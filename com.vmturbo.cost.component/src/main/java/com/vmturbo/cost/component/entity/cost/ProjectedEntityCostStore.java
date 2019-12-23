package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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


    ProjectedEntityCostStore(@Nonnull RepositoryClient repositoryClient, @Nonnull
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
     *
     * @param filter the input filter.
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     *         associated projected costs after filters will not have an entry in the map.
     */
    @Nonnull
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
