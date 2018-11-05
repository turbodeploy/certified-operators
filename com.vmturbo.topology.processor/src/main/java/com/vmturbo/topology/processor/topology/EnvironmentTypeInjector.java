package com.vmturbo.topology.processor.topology;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.TopologyEntity;
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
            SDKProbeType.AZURE_COST);

    private static final Logger logger = LogManager.getLogger();

    private final TargetStore targetStore;

    public EnvironmentTypeInjector(@Nonnull final TargetStore targetStore) {
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Set the environment type of entities in a {@link TopologyGraph}.
     *
     * @param topologyGraph The {@link TopologyGraph}, after all stitching.
     * @return An {@link InjectionSummary} describing the changes made.
     */
    @Nonnull
    public InjectionSummary injectEnvironmentType(@Nonnull final TopologyGraph topologyGraph) {
        final Map<EnvironmentType, Integer> envTypeCounts = new EnumMap<>(EnvironmentType.class);
        final AtomicInteger conflictingTypeCount = new AtomicInteger(0);

        // Pre-compute the targets whose entities should count as "CLOUD", because it's far more
        // efficient than doing it for each entity.
        final Set<Long> cloudTargetIds = targetStore.getAll().stream()
                .map(Target::getId)
                .filter(targetId -> targetStore.getProbeTypeForTarget(targetId)
                    .map(CLOUD_PROBE_TYPES::contains)
                    .orElse(false))
                .collect(Collectors.toSet());
        topologyGraph.entities().forEach(topoEntity -> {

            final EnvironmentType envType = getEnvironmentType(topoEntity, cloudTargetIds);

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
                            topoEntity.getDisplayName() , topoEntity.getOid(),
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
                    // TODO (roman, Oct 25 2018): The type of probe that discovered the entity
                    // may not be enough. For example, an application discovered on a VM in AWS
                    // may not be discovered by a cloud probe, but it should still be considered
                    // a cloud application.
                    return EnvironmentType.ON_PREM;
                }
            case RESERVATION_ORIGIN:
                // It's not clear if we ever need reservations in the cloud.
                return EnvironmentType.ON_PREM;
            case PLAN_ORIGIN:
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
     * A summary of what happened during the injection of environment types into a
     * {@link TopologyGraph}.
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
