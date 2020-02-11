package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import gnu.trove.set.TLongSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Responsible for determining which {@link EnvironmentType} a particular {@link TopologyEntity}
 * should have, based on its origin and, possibly, the entities its related to.
 */
public class EnvironmentTypeInjector {

    /**
     * Entities discovered by these probes should be considered CLOUD entities.
     */
    private static final Set<SDKProbeType> CLOUD_PROBE_TYPES = ImmutableSet.of(
        SDKProbeType.AWS,
        SDKProbeType.AWS_COST,
        SDKProbeType.AWS_BILLING,
        SDKProbeType.AZURE,
        SDKProbeType.AZURE_COST,
        SDKProbeType.AZURE_EA,
        SDKProbeType.AZURE_SERVICE_PRINCIPAL,
        SDKProbeType.AZURE_STORAGE_BROWSE,
        SDKProbeType.GCP,
        SDKProbeType.GCP_COST);

    private static final Logger logger = LogManager.getLogger();

    private final TargetStore targetStore;

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
        final EntityTypeCache cache = new EntityTypeCache();
        topologyGraph.entities().forEach(topoEntity -> {
            final EnvironmentType envType;
            final boolean discoveredByAppOrContainer = topoEntity.getDiscoveringTargetIds()
                .anyMatch(appContainerTargetIds::contains);
            if (discoveredByAppOrContainer) {
                envType = computeEnvironmentTypeByProviders(topoEntity, cloudTargetIds, cache);
            } else {
                envType = getEnvironmentType(topoEntity, cloudTargetIds);
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
    private EnvironmentType getEnvironmentType(@Nonnull final TopologyEntity entity,
                                               @Nonnull final Set<Long> cloudTargetIds) {
        final Optional<Origin> originOpt = entity.getOrigin();
        if (!originOpt.isPresent()) {
            // We expect origin to be set in the input entities by the time they make it
            // to the injector.
            return EnvironmentType.UNKNOWN_ENV;
        }

        final Origin origin = originOpt.get();
        switch (origin.getOriginTypeCase()) {
            case DISCOVERY_ORIGIN:
                final boolean discoveredByCloud = entity.getDiscoveringTargetIds()
                    .anyMatch(cloudTargetIds::contains);
                if (discoveredByCloud) {
                    return EnvironmentType.CLOUD;
                } else {
                    return EnvironmentType.ON_PREM;
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
     * GUEST_OS_PROCESSES, etc.). Some providers (other entities) of this target entity may have
     * different env. types, and it influences on the target entity env. type.
     * - If all env. types of all leaf providers are the same (leafProvidersTypes.size()==1), this env.
     *   type goes to the target entity;
     * - If the set 'leafProvidersTypes' contains HYBRID env. type, it means that target entity
     *   has HYBRID type as well;
     * - If the set 'leafProvidersTypes' contains both ON_PREM and CLOUD types, it means that target
     *   entity has HYBRID env. type.
     * @param entity - target entity
     * @param cloudTargetIds - a set of targets IDs, that considered as cloud-based.
     * @param cache - Cache to avoid multiple traversals of the same providers.
     * @return an environment type of the target entity.
     */
    @Nonnull
    private EnvironmentType computeEnvironmentTypeByProviders(@Nonnull TopologyEntity entity,
                                                              @Nonnull Set<Long> cloudTargetIds,
                                                              @Nonnull EntityTypeCache cache) {
        return cache.computeIfAbsent(entity.getOid(), () -> {
            final EnvironmentType ret;
            if (entity.getProviders().size() == 0) {
                ret = getEnvironmentType(entity, cloudTargetIds);
            } else {
                EnvironmentType combinedType = null;
                for (TopologyEntity p : entity.getProviders()) {
                    final EnvironmentType providerEnvType =
                        computeEnvironmentTypeByProviders(p, cloudTargetIds, cache);
                    if (combinedType == null || combinedType == EnvironmentType.UNKNOWN_ENV) {
                        // If the combined type is unset or UNKNOWN, override it completely.
                        combinedType = providerEnvType;
                    } else if (combinedType != providerEnvType && providerEnvType != EnvironmentType.UNKNOWN_ENV) {
                        // If this provider's type is not UNKNOWN and not the same as the current
                        // combined type, that means the entity has a HYBRID environment.
                        combinedType = EnvironmentType.HYBRID;
                        // Once we know it's hybrid we don't care about the rest of the providers.
                        break;
                    }
                }
                ret = combinedType;
            }
            return ret;
        });
    }

    /**
     * A cache for already-computed environment types.
     * We use this to avoid repeating traversals when recursively traversing providers to calculate
     * an entity's environment type
     * (see {@link EnvironmentTypeInjector#computeEnvironmentTypeByProviders(TopologyEntity, Set, EntityTypeCache)}.
     */
    private static class EntityTypeCache {

        /**
         * Map from (EnvironmentType) -> (oid set).
         * The "oid set" represents the entities that have that entity type.
         *
         * <p>We use a roaring bitmap to reduce the memory
         * footprint. The memory required to cache 200k entries is ~13.5MB when using a {@link Set},
         * 3.7MB when using a {@link TLongSet}, and 0.5MB when using the bitmap.
         */
        private final EnumMap<EnvironmentType, Roaring64NavigableMap> providersByEnv;

        EntityTypeCache() {
            providersByEnv = new EnumMap<>(EnvironmentType.class);
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
            for (Entry<EnvironmentType, Roaring64NavigableMap> entry : providersByEnv.entrySet()) {
                if (entry.getValue().contains(id)) {
                    return entry.getKey();
                }
            }

            // If not found in the cache, calculate the environment type, and then
            // update the cache.
            final EnvironmentType envTypeSet = envTypeSupplier.get();
            final Roaring64NavigableMap mapForEnv =
                providersByEnv.computeIfAbsent(envTypeSet, k -> new Roaring64NavigableMap());
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
