package com.vmturbo.topology.processor.topology;

import static com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil.CLOUD_PROBE_TYPES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import gnu.trove.set.TLongSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Responsible for determining which {@link EnvironmentType} a particular {@link TopologyEntity}
 * should have, based on its origin and, possibly, the entities its related to.
 */
public class EnvironmentTypeInjector {

    private static final Logger logger = LogManager.getLogger();

    private final TargetStore targetStore;

    /**
     * Provides functions for traversing related entities in the topology when looking
     * up the environment type of an entity.
     * <p/>
     * Key: Integer EntityType of an entity.
     * Value: A function that given an entity, returns a Collection of connected entities in
     *         the topology to traverse when searching for environment type. By default, we
     *         traverse provider entities for entity types not explicitly in the map.
     */
    private static final Map<Integer, Function<TopologyEntity, Collection<TopologyEntity>>> ALTERNATE_TRAVERSAL_MAP =
        ImmutableMap.of(
            // ContainerSpecs are connected to their on-prem/cloud infrastructure through the Containers
            // they aggregate.
            EntityType.CONTAINER_SPEC_VALUE, TopologyEntity::getAggregatedEntities,
            // WorkloadControllers are connected to their on-prem/cloud infrastructure through the ContainerPods
            // that consume from them.
            EntityType.WORKLOAD_CONTROLLER_VALUE, TopologyEntity::getConsumers,
            // ContainerPlatformClusters have aggregated on-prem/cloud VM entities.
            EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE, EnvironmentTypeInjector::getAggregatedVMs,
            // Namespaces are aggregated by ContainerPlatformClusters. Traverse ContainerPlatformCluster
            // to find out connected on-prem/cloud infrastructure so that namespace will have consistent
            // environment type with container platform cluster.
            EntityType.NAMESPACE_VALUE, TopologyEntity::getAggregators
        );

    public EnvironmentTypeInjector(@Nonnull final TargetStore targetStore) {
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Set the environment type of entities in a {@link TopologyGraph<TopologyEntity>}.
     *
     * @param topologyGraph The {@link TopologyGraph<TopologyEntity>}, after all stitching.
     * @return An {@link InjectionSummary} describing the changes made.
     */
    @Nonnull
    public InjectionSummary injectEnvironmentType(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        final Map<EnvironmentType, Integer> envTypeCounts = new EnumMap<>(EnvironmentType.class);
        final AtomicInteger conflictingTypeCount = new AtomicInteger(0);
        // Pre-compute the targets whose entities should count as "CLOUD", because it's far more
        // efficient than doing it for each entity.
        List<Target> targets = new ArrayList<>(targetStore.getAll());
        final Set<Long> cloudTargetIds = targets.stream()
            .map(Target::getId)
            .filter(targetId -> targetStore.getProbeTypeForTarget(targetId)
                .map(CLOUD_PROBE_TYPES::contains)
                .orElse(false))
            .collect(Collectors.toSet());
        // Pre-compute the targets that produces containers/apps
        final Set<Long> appContainerTargetIds = targets.stream()
            .map(Target::getId)
            .filter(targetId -> targetStore.getProbeCategoryForTarget(targetId)
                .map(ProbeCategory::isAppOrContainerCategory)
                .orElse(false))
            .collect(Collectors.toSet());
        final EnvironmentTypeCache cache = new EnvironmentTypeCache();
        topologyGraph.entities().forEach(topoEntity -> {
            EnvironmentType envType;
            final boolean discoveredOnlyByAppOrContainer =
                    topoEntity.getDiscoveringTargetIds().count() > 0
                    && topoEntity.getDiscoveringTargetIds().allMatch(appContainerTargetIds::contains);
            if (discoveredOnlyByAppOrContainer) {
                envType = computeEnvironmentType(topoEntity, cloudTargetIds, appContainerTargetIds,
                        cache, new HashSet<>());
                if (envType == EnvironmentType.UNKNOWN_ENV) {
                    envType = EnvironmentType.HYBRID;
                }
            } else {
                envType = getEnvironmentType(topoEntity, cloudTargetIds, appContainerTargetIds);
            }
            // We shouldn't have entities with env type already set
            // unless we're in plan-over-plan.
            if (topoEntity.getTopologyEntityDtoBuilder().hasEnvironmentType()) {
                final EnvironmentType existingType =
                    topoEntity.getTopologyEntityDtoBuilder().getEnvironmentType();
                // If environment type is already set, it REALLY shouldn't be different from what
                // the injector determines it should be.
                if (existingType != envType) {
                    // If it's explicitly set to unknown, and we found a "proper" environment type
                    // to set it to, then it's probably safe to do so.
                    if (existingType == EnvironmentType.UNKNOWN_ENV) {
                        logger.info("Entity {} (id: {}) Overriding explicitly-set unknown " +
                                "environment type with {}", topoEntity.getDisplayName(),
                            topoEntity.getOid(), envType);
                        topoEntity.getTopologyEntityDtoBuilder().setEnvironmentType(envType);
                        envTypeCounts.compute(envType, (k, curCount) ->
                            (curCount == null ? 0 : curCount) + 1);
                    } else {
                        // If it's explicitly set to something different than what we found, leave
                        // it as is. This shouldn't happen.
                        logger.error("Entity {} (id: {}) already has environment type set to: {}." +
                                " Injector calculated: {}. Not overriding.",
                            topoEntity.getDisplayName(), topoEntity.getOid(),
                            topoEntity.getTopologyEntityDtoBuilder().getEnvironmentType(), envType);
                        conflictingTypeCount.incrementAndGet();
                    }
                }
            } else {
                logger.debug("Entity {} (id: {}) - setting environment type to {}",
                    topoEntity.getDisplayName(), topoEntity.getOid(), envType);
                topoEntity.getTopologyEntityDtoBuilder().setEnvironmentType(envType);
                envTypeCounts.compute(envType, (k, curCount) ->
                    (curCount == null ? 0 : curCount) + 1);
            }
        });

        final int unknownCount = envTypeCounts.getOrDefault(EnvironmentType.UNKNOWN_ENV, 0);
        // Don't need to send duplicate information.
        // We keep unknown count separately to make it easier to interact with (since it may need to
        // trigger a warning).
        envTypeCounts.remove(EnvironmentType.UNKNOWN_ENV);
        return new InjectionSummary(unknownCount, conflictingTypeCount.get(), envTypeCounts);
    }

    @Nonnull
    private static Collection<TopologyEntity> getAggregatedVMs(@Nonnull TopologyEntity containerClusterEntity) {
        return containerClusterEntity.getAggregatedEntities().stream()
            .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .collect(Collectors.toList());
    }

    @Nonnull
    private EnvironmentType getEnvironmentType(@Nonnull final TopologyEntity entity,
                                               @Nonnull final Set<Long> cloudTargetIds,
                                               @Nonnull final Set<Long> appContainerTargetIds) {
        final Optional<Origin> originOpt = entity.getOrigin();
        if (!originOpt.isPresent()) {
            // We expect origin to be set in the input entities by the time they make it
            // to the injector.
            return EnvironmentType.UNKNOWN_ENV;
        }

        final Origin origin = originOpt.get();
        switch (origin.getOriginTypeCase()) {
            case DISCOVERY_ORIGIN:
                final boolean discoveredByCloud = entity.getDiscoveringTargetIds().count() > 0
                        && entity.getDiscoveringTargetIds().anyMatch(cloudTargetIds::contains);
                if (discoveredByCloud) {
                    return EnvironmentType.CLOUD;
                } else {
                    final boolean discoveredOnlyByAppOrContainer =
                            entity.getDiscoveringTargetIds().count() > 0
                            && entity.getDiscoveringTargetIds().allMatch(appContainerTargetIds::contains);
                    if (discoveredOnlyByAppOrContainer) {
                        // Set as UNKNOWN for now so. If later there will be no CLOUD/ON_PREM connected
                        // entities - we will set this to HYBRID.
                        return EnvironmentType.UNKNOWN_ENV;
                    } else {
                        return EnvironmentType.ON_PREM;
                    }
                }
            case RESERVATION_ORIGIN:
                // It's not clear if we ever need reservations in the cloud.
                return EnvironmentType.ON_PREM;
            case PLAN_SCENARIO_ORIGIN:
                // TODO (roman, Oct 25 2018): Entities that are added as part of a plan may be
                // cloud entities - e.g. if we're trying to simulate the impact of adding a bunch
                // of instances to the AWS environment.
                //
                // For example, for a cloned entity we may want to copy the environment type of the
                // entity being cloned. However, there are probably corner cases for different
                // types of cloud and/or hybrid plans - e.g. a cloud migrate plan may have a
                // cloned ON_PREM entity that's being forced into the CLOUD.
                return EnvironmentType.ON_PREM;
            default:
                return EnvironmentType.UNKNOWN_ENV;
        }
    }

    /**
     * The method defines an environment type (CLOUD, ON_PREM, etc.) of an entity, which was
     * discovered by a probe that can be considered as 'application or container' (CLOUD_NATIVE,
     * GUEST_OS_PROCESSES, etc.). Some connections (other entities) of this target entity may have
     * different env. types, and it influences on the target entity env. type.
     * - If all env. types of all leaf connections are the same (leafConnectionTypes.size()==1), this env.
     *   type goes to the target entity;
     * - If the set 'leafConnectionTypes' contains HYBRID env. type, it means that target entity
     *   has HYBRID type as well;
     * - If the set 'leafConnectionTypes' contains both ON_PREM and CLOUD types, it means that target
     *   entity has HYBRID env. type.
     * - By default we traverse provider relationships, but we allow override of this behavior through
     *   the ALTERNATE_TRAVERSAL_MAP. We skip traversing connections from entity types outside the
     *   ALTERNATE_TRAVERSAL_MAP to inside the ALTERNATE_TRAVERSAL_MAP to prevent infinite traversal.
     *
     * @param entity target entity
     * @param cloudTargetIds a set of targets IDs, that considered as cloud-based.
     * @param appContainerTargetIds a set of targets IDs, that considered as APM / container targets.
     * @param cache Cache to avoid multiple traversals of the same connections.
     * @param traversed The set of entities traversed so far during the environment type computation.
     * @return an environment type of the target entity.
     */
    @Nonnull
    private EnvironmentType computeEnvironmentType(@Nonnull TopologyEntity entity,
                                                   @Nonnull Set<Long> cloudTargetIds,
                                                   @Nonnull Set<Long> appContainerTargetIds,
                                                   @Nonnull EnvironmentTypeCache cache,
                                                   @Nonnull Set<Long> traversed) {
        return cache.computeIfAbsent(entity.getOid(), () -> {
            if (traversed.contains(entity.getOid())) {
                logger.error("Cycle detected at entity {} of type {} during "
                    + "computeEnvironmentType traversal. Ending this leg of traversal.",
                    entity.getOid(), EntityType.forNumber(entity.getEntityType()));
                return getEnvironmentType(entity, cloudTargetIds, appContainerTargetIds);
            }

            traversed.add(entity.getOid());
            final Collection<TopologyEntity> connections = ALTERNATE_TRAVERSAL_MAP
                .getOrDefault(entity.getEntityType(), TopologyEntity::getProviders)
                .apply(entity);

            EnvironmentType combinedType = null;
            for (TopologyEntity connection : connections) {
                if (shouldSkipTraversal(entity, connection)) {
                    continue;
                }

                final EnvironmentType connectedEnvType =
                    computeEnvironmentType(connection, cloudTargetIds, appContainerTargetIds, cache, traversed);
                if (combinedType == null || combinedType == EnvironmentType.UNKNOWN_ENV) {
                    // If the combined type is unset or UNKNOWN, override it completely.
                    combinedType = connectedEnvType;
                } else if (combinedType != connectedEnvType && connectedEnvType != EnvironmentType.UNKNOWN_ENV) {
                    // If this connections's type is not UNKNOWN and not the same as the current
                    // combined type, that means the entity has a HYBRID environment.
                    combinedType = EnvironmentType.HYBRID;
                    // Once we know it's hybrid we don't care about the rest of the connections.
                    break;
                }
            }

            // We failed to determine environment type through any connections, so perform lookup on ourselves.
            if (combinedType == null) {
                combinedType = getEnvironmentType(entity, cloudTargetIds, appContainerTargetIds);
            }
            return combinedType;
        });
    }

    /**
     * During environment type computation, check whether to skip the traversal between
     * two entities. We skip traversals from outside-->inside the alternate traversal
     * subgraph.
     * <p/>
     * Typically we wish to traverse provider relationships when computing environment type,
     * but certain entity types are not connected to the infrastructure through provider
     * relationships. For example, WORKLOAD_CONTROLLER entity types are connected through
     * the Pods that consume quota commodities from the controllers. To prevent an infinite
     * loop where we traverse Controller Consumers -> Pod Providers -> Controller Consumers
     * etc. we prevent traversals from outside the alternate traversal subgraph to the inside.
     * Searching inside the alternate traversal subgraph will not lead to finding the
     * infrastructure part of the supply chain in any case, so there is no use in traversing
     * from outside to inside.
     *
     * @param connectionStart For a directed edge in the graph, this entity is the starting
     *                        node.
     * @param connectionEnd For a directed edge in the graph, this entity is the destination
     *                      node.
     * @return Whether to skip the traversal of the edge from the start to the end entities.
     */
    private boolean shouldSkipTraversal(@Nonnull final TopologyEntity connectionStart,
                                        @Nonnull final TopologyEntity connectionEnd) {
        return ALTERNATE_TRAVERSAL_MAP.containsKey(connectionEnd.getEntityType())
            && !ALTERNATE_TRAVERSAL_MAP.containsKey(connectionStart.getEntityType());
    }

    /**
     * A cache for already-computed environment types.
     * We use this to avoid repeating traversals when recursively traversing providers to calculate
     * an entity's environment type
     * (see {@link EnvironmentTypeInjector#computeEnvironmentType(TopologyEntity, Set, Set, EnvironmentTypeCache, Set)}.
     */
    private static class EnvironmentTypeCache {

        /**
         * Map from (EnvironmentType) -> (oid set).
         * The "oid set" represents the entities that have that entity type.
         *
         * <p>We use a roaring bitmap to reduce the memory
         * footprint. The memory required to cache 200k entries is ~13.5MB when using a {@link Set},
         * 3.7MB when using a {@link TLongSet}, and 0.5MB when using the bitmap.
         */
        private final EnumMap<EnvironmentType, Roaring64NavigableMap> entitiesByEnv;

        EnvironmentTypeCache() {
            entitiesByEnv = new EnumMap<>(EnvironmentType.class);
        }

        /**
         * Check the cache for the environment type, populating the cache
         * if no existing entry is present.
         *
         * @param id The id of the entity.
         * @param envTypeSupplier A supplier that will calculate the environment type for the entity
         *                        if it's not already cached.
         * @return The {@link EnvironmentType} for the entity.
         */
        public EnvironmentType computeIfAbsent(final long id,
                                     @Nonnull final Supplier<EnvironmentType> envTypeSupplier) {
            // We iterate over all keys and search each one. This is fast, because the total number
            // of possible keys is 3-4.
            for (Entry<EnvironmentType, Roaring64NavigableMap> entry : entitiesByEnv.entrySet()) {
                if (entry.getValue().contains(id)) {
                    return entry.getKey();
                }
            }

            // If not found in the cache, calculate the environment type, and then
            // update the cache.
            final EnvironmentType envTypeSet = envTypeSupplier.get();
            final Roaring64NavigableMap mapForEnv =
                entitiesByEnv.computeIfAbsent(envTypeSet, k -> new Roaring64NavigableMap());
            mapForEnv.addLong(id);
            return envTypeSet;
        }
    }

    /**
     * A summary of what happened during the injection of environment types into a
     * {@link TopologyGraph<TopologyEntity>}.
     */
    public static class InjectionSummary {

        private final int unknownCount;

        private final int conflictingTypeCount;

        private final Map<EnvironmentType, Integer> envTypeCounts;

        public InjectionSummary(final int unknownCount,
                                final int conflictingTypeCount,
                                final Map<EnvironmentType, Integer> envTypeCounts) {
            this.unknownCount = unknownCount;
            this.conflictingTypeCount = conflictingTypeCount;
            this.envTypeCounts = envTypeCounts;
        }

        /**
         * @return The number of entities for which the type could not be determined.
         */
        public int getUnknownCount() {
            return unknownCount;
        }

        /**
         * @return The number of entities that already had a non-UNKNOWN type set.
         */
        public int getConflictingTypeCount() {
            return conflictingTypeCount;
        }

        /**
         * @return (env type) -> (num of entities the env type was injected into)
         */
        @Nonnull
        public Map<EnvironmentType, Integer> getEnvTypeCounts() {
            return Collections.unmodifiableMap(envTypeCounts);
        }
    }

}
