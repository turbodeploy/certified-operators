package com.vmturbo.topology.processor.stitching;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A Factory for constructing concrete {@link StitchingScope}s for use in
 * {@link PostStitchingOperation}s.
 *
 * These scopes determine which entities in the {@link TopologyGraph<TopologyEntity>}
 * are fed to the {@link PostStitchingOperation#performOperation(Stream, EntitySettingsCollection, EntityChangesBuilder)}
 * method.
 */
public class PostStitchingOperationScopeFactory implements StitchingScopeFactory<TopologyEntity> {

    private static final Logger logger = LogManager.getLogger();
    private final TopologyGraph<TopologyEntity> topologyGraph;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final CpuCapacityStore cpuCapacitystore;

    public PostStitchingOperationScopeFactory(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                              @Nonnull final ProbeStore probeStore,
                                              @Nonnull final TargetStore targetStore,
                                              @Nonnull final CpuCapacityStore cpuCapacityStore) {
        this.topologyGraph = Objects.requireNonNull(topologyGraph);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.cpuCapacitystore = Objects.requireNonNull(cpuCapacityStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> globalScope() {
        return new GlobalStitchingScope(topologyGraph);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> probeScope(@Nonnull final String probeTypeName) {
        return new ProbeTypeStitchingScope(topologyGraph, probeTypeName, probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> entityTypeScope(@Nonnull final EntityType entityType) {
        return new EntityTypeStitchingScope(topologyGraph, entityType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> multiEntityTypesScope(@Nonnull final List<EntityType> entityTypes) {
        return new MultiEntityTypesStitchingScope(topologyGraph, entityTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> containsAllEntityTypesScope(@Nonnull final List<EntityType> entityTypes) {
        return new ContainsAllEntityTypesStitchingScope(topologyGraph, entityTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> probeEntityTypeScope(@Nonnull final String probeTypeName,
                                                 @Nonnull final EntityType entityType) {
        return new ProbeEntityTypeStitchingScope(topologyGraph, probeTypeName, entityType,
            probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> multiProbeEntityTypeScope(@Nonnull final Set<String> probeTypeNames,
                                                               @Nonnull final EntityType entityType) {
        return new MultiProbeEntityTypeStitchingScope(topologyGraph, probeTypeNames, entityType,
                probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> probeCategoryEntityTypeScope(@Nonnull final ProbeCategory probeCategory,
                                                         @Nonnull final EntityType entityType) {
        return new ProbeCategoryEntityTypeStitchingScope(topologyGraph, probeCategory, entityType,
            probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<TopologyEntity> multiProbeCategoryEntityTypeScope(
            @Nonnull final Set<ProbeCategory> probeCategories,
            @Nonnull final EntityType entityType) {
        return new MultiProbeCategoryEntityTypeStitchingScope(topologyGraph, probeCategories,
                entityType, probeStore, targetStore);
    }

    @Override
    public StitchingScope<TopologyEntity> missingDerivedTargetEntityTypeScope(
        @Nonnull final String parentProbeType,
        @Nonnull final String childProbeType,
        @Nonnull final EntityType entityType) {
        return new MissingDerivedTargetEntityTypeScope(topologyGraph, parentProbeType,
            childProbeType, entityType, probeStore, targetStore);
    }

    @Override
    public StitchingScope<TopologyEntity> hasAndLacksProbeCategoryEntityTypeStitchingScope(
            @Nonnull final Set<ProbeCategory> owningProbeCategories,
            @Nonnull final Set<ProbeCategory> missingProbeCategories,
            @Nonnull final EntityType entityType) {
        return new HasAndLacksProbeCategoryEntityTypeStitchingScope(topologyGraph, owningProbeCategories, missingProbeCategories,
                entityType, probeStore, targetStore);
    }

    public TopologyGraph<TopologyEntity> getTopologyGraph() {
        return topologyGraph;
    }

    /**
     * The base class for calculation scopes. Takes a {@link StitchingContext}.
     */
    private static abstract class BaseStitchingScope implements StitchingScope<TopologyEntity> {
        private final TopologyGraph<TopologyEntity> topologyGraph;

        public BaseStitchingScope(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
            this.topologyGraph = Objects.requireNonNull(topologyGraph);
        }

        public TopologyGraph<TopologyEntity> getTopologyGraph() {
            return topologyGraph;
        }
    }

    /**
     * A calculation scope for applying a calculation globally to all entities.
     */
    private static class GlobalStitchingScope extends BaseStitchingScope {
        public GlobalStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph) {
            super(topologyGraph);
        }

        @Override
        @Nonnull
        public Stream<TopologyEntity> entities() {
            return getTopologyGraph().entities();
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a specific type of probe.
     */
    private static class ProbeTypeStitchingScope extends BaseStitchingScope {

        private final String probeTypeName;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                       @Nonnull final String probeTypeName,
                                       @Nonnull final ProbeStore probeStore,
                                       @Nonnull final TargetStore targetStore) {
            super(topologyGraph);
            this.probeTypeName = Objects.requireNonNull(probeTypeName);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            final Optional<Long> optionalProbeId = probeStore.getProbeIdForType(probeTypeName);
            if (!optionalProbeId.isPresent()) {
                logger.debug("Unable to retrieve entities for " + probeTypeName +
                    " because no probe of that type is currently registered.");
                return Stream.empty();
            }

            final long probeId = optionalProbeId.get();
            final Set<Long> probeTargetIds = targetStore.getProbeTargets(probeId).stream()
                .map(Target::getId)
                .collect(Collectors.toSet());
            return getTopologyGraph().entities()
                .filter(TopologyEntity::hasDiscoveryOrigin)
                .filter(entity -> entity.getDiscoveryOrigin().get()
                                .getDiscoveredTargetDataMap().keySet().stream()
                                .anyMatch(probeTargetIds::contains));

        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific {@link EntityType}.
     */
    private static class EntityTypeStitchingScope extends BaseStitchingScope {

        private final EntityType entityType;

        public EntityTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                        @Nonnull final EntityType entityType) {
            super(topologyGraph);
            this.entityType = Objects.requireNonNull(entityType);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            return getTopologyGraph().entitiesOfType(entityType);
        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific {@link EntityType}.
     */
    private static class MultiEntityTypesStitchingScope extends BaseStitchingScope {

        private final List<EntityType> entityTypes;

        public MultiEntityTypesStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                              @Nonnull final List<EntityType> entityTypes) {
            super(topologyGraph);
            this.entityTypes = Objects.requireNonNull(entityTypes);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            return entityTypes
                    .stream()
                    .flatMap(entityType -> getTopologyGraph().entitiesOfType(entityType));
        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific {@link EntityType}.
     */
    private static class ContainsAllEntityTypesStitchingScope extends BaseStitchingScope {

        private final List<EntityType> entityTypes;

        public ContainsAllEntityTypesStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                @Nonnull final List<EntityType> entityTypes) {
            super(topologyGraph);
            this.entityTypes = Objects.requireNonNull(entityTypes);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            // return empty stream if entities for any EntityType are not available
            if (entityTypes.stream().anyMatch(entityType -> !getTopologyGraph()
                    .entitiesOfType(entityType).findAny().isPresent())) {
                return Stream.empty();
            }

            return entityTypes.stream().flatMap(entityType ->
                    getTopologyGraph().entitiesOfType(entityType));
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a specific type of probe
     * with a specific {@link EntityType}.
     */
    private static class ProbeEntityTypeStitchingScope extends BaseStitchingScope {

        private final String probeTypeName;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeEntityTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                             @Nonnull final String probeTypeName,
                                             @Nonnull final EntityType entityType,
                                             @Nonnull final ProbeStore probeStore,
                                             @Nonnull final TargetStore targetStore) {
            super(topologyGraph);
            this.probeTypeName = Objects.requireNonNull(probeTypeName);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            final Optional<Long> optionalProbeId = probeStore.getProbeIdForType(probeTypeName);
            if (!optionalProbeId.isPresent()) {
                logger.debug("Unable to retrieve entities for " + probeTypeName +
                    " because no probe of that type is currently registered.");
                return Stream.empty();
            }

            final long probeId = optionalProbeId.get();
            final Set<Long> probeTargetIds = targetStore.getProbeTargets(probeId).stream()
                .map(Target::getId)
                .collect(Collectors.toSet());
            return getTopologyGraph().entitiesOfType(entityType)
                .filter(TopologyEntity::hasDiscoveryOrigin)
                .filter(entity -> entity.getDiscoveryOrigin().get()
                                .getDiscoveredTargetDataMap().keySet().stream()
                                .anyMatch(probeTargetIds::contains));
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a specific set of
     * probes with a specific {@link EntityType}.
     */
    private static class MultiProbeEntityTypeStitchingScope extends BaseStitchingScope {

        private final Set<String> probeTypeNames;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public MultiProbeEntityTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                             @Nonnull final Set<String> probeTypeNames,
                                             @Nonnull final EntityType entityType,
                                             @Nonnull final ProbeStore probeStore,
                                             @Nonnull final TargetStore targetStore) {
            super(topologyGraph);
            this.probeTypeNames = Objects.requireNonNull(probeTypeNames);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {

            final Set<Long> probeTargetIds = new HashSet<>();

            // iterate over probe types
            for (String probeTypeName: probeTypeNames) {

                // get probe ID
                final Optional<Long> optionalProbeId = probeStore.getProbeIdForType(probeTypeName);
                if (optionalProbeId.isPresent()) {
                    final long probeId = optionalProbeId.get();

                    // get all targets for the specified probe ID and add it to the probeTargetIds set
                    final List<Target> probeTargets = targetStore.getProbeTargets(probeId);
                    for (Target target : probeTargets) {
                        probeTargetIds.add(target.getId());
                    }
                } else {
                    logger.debug("Unable to retrieve entities for " + probeTypeName +
                            " because no probe of that type is currently registered.");
                }
            }

            if (probeTargetIds.isEmpty()) {
                // return immediately if we didn't find any target (performance reasons)
                return Stream.empty();
            } else {
                // get entities from those targets
                // this is doing a linear scan over all entities of that type
                // regardless of the target they came from
                // note: optimize it in future, if it's starting to be a bottleneck
                return getTopologyGraph().entitiesOfType(entityType)
                        .filter(TopologyEntity::hasDiscoveryOrigin)
                        .filter(entity -> entity.getDiscoveryOrigin().get()
                                        .getDiscoveredTargetDataMap().keySet().stream()
                                        .anyMatch(probeTargetIds::contains));
            }
        }

    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a probe category
     * with a specific {@link EntityType}.
     */
    private static class ProbeCategoryEntityTypeStitchingScope extends BaseStitchingScope {

        private final ProbeCategory probeCategory;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeCategoryEntityTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                                     @Nonnull final ProbeCategory probeCategory,
                                                     @Nonnull final EntityType entityType,
                                                     @Nonnull final ProbeStore probeStore,
                                                     @Nonnull final TargetStore targetStore) {
            super(topologyGraph);
            this.probeCategory = Objects.requireNonNull(probeCategory);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            final Set<Long> probeCategoryTargetIds = probeStore.getProbeIdsForCategory(probeCategory).stream()
                 .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream()
                     .map(Target::getId))
                 .collect(Collectors.toSet());

            return getTopologyGraph().entitiesOfType(entityType)
                .filter(TopologyEntity::hasDiscoveryOrigin)
                .filter(entity -> entity.getDiscoveryOrigin().get()
                                .getDiscoveredTargetDataMap().keySet().stream()
                                .anyMatch(probeCategoryTargetIds::contains));
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by some categories but not discovered by other probe categories
     * with a specific {@link EntityType}. i.e. an entity has this probe categories will not pass
     */
    private static class HasAndLacksProbeCategoryEntityTypeStitchingScope extends BaseStitchingScope {

        private final Set<ProbeCategory> owningProbeCategories;
        private final Set<ProbeCategory> missingProbeCategories;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        HasAndLacksProbeCategoryEntityTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                                     @Nonnull final Set<ProbeCategory> owningProbeCategories,
                                                     @Nonnull final Set<ProbeCategory> missingProbeCategories,
                                                     @Nonnull final EntityType entityType,
                                                     @Nonnull final ProbeStore probeStore,
                                                     @Nonnull final TargetStore targetStore) {
            super(topologyGraph);
            this.owningProbeCategories = Objects.requireNonNull(owningProbeCategories);
            this.missingProbeCategories = Objects.requireNonNull(missingProbeCategories);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            final Set<Long> owningProbeCategoryTargetIds = getTargetIdsForProbeCategory(owningProbeCategories);
            final Set<Long> missingProbeCategoryTargetIds = getTargetIdsForProbeCategory(missingProbeCategories);
            return getTopologyGraph().entitiesOfType(entityType)
                    .filter(TopologyEntity::hasDiscoveryOrigin)
                    .filter(entity -> {
                        Set<Long> targetsIds = entity.getDiscoveryOrigin().get().getDiscoveredTargetDataMap().keySet();
                        return !Collections.disjoint(owningProbeCategoryTargetIds, targetsIds)
                                && Collections.disjoint(missingProbeCategoryTargetIds, targetsIds);
                    }
                    );
        }

        /**
         * Get the IDs of targets that have given probe categories.
         * @param probeCategories the categories to find.
         * @return IDs of targets that have given probe categories.
         */
        private Set<Long> getTargetIdsForProbeCategory(Set<ProbeCategory> probeCategories) {

            return probeCategories.stream().flatMap(
                    probeCategory -> probeStore.getProbeIdsForCategory(probeCategory).stream()
                    .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream())
                    .flatMap(target -> Stream.concat(targetStore.getDerivedTargetIds(target.getId()).stream(),
                            Stream.of(Long.valueOf(target.getId()))))
            ).collect(Collectors.toSet());
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a probe category
     * with a specific {@link EntityType}.
     */
    private static class MultiProbeCategoryEntityTypeStitchingScope extends BaseStitchingScope {

        private final Set<ProbeCategory> probeCategories;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public MultiProbeCategoryEntityTypeStitchingScope(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                                     @Nonnull final Set<ProbeCategory> probeCategories,
                                                     @Nonnull final EntityType entityType,
                                                     @Nonnull final ProbeStore probeStore,
                                                     @Nonnull final TargetStore targetStore) {
            super(topologyGraph);
            this.probeCategories = Objects.requireNonNull(probeCategories);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            final Set<Long> probeCategoryTargetIds = probeCategories.stream().flatMap(category ->
                    probeStore.getProbeIdsForCategory(category).stream())
                    .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream()
                            .map(Target::getId))
                    .collect(Collectors.toSet());

            return getTopologyGraph().entitiesOfType(entityType)
                    .filter(TopologyEntity::hasDiscoveryOrigin)
                    .filter(entity -> entity.getDiscoveryOrigin().get()
                                    .getDiscoveredTargetDataMap().keySet().stream()
                                    .anyMatch(probeCategoryTargetIds::contains));
        }
    }

    /**
     * A scope for finding entities that have at least one discovering target of a particular type
     * (parentProbeType) that don't have a derived target of a specific type (childProbeType).
     * For example, we use this scope to identify VC storages that don't have storage browsing
     * enabled for {@link
     * com.vmturbo.stitching.poststitching.ProtectSharedStorageWastedFilesPostStitchingOperation}
     */
    private static class MissingDerivedTargetEntityTypeScope extends BaseStitchingScope {

        private final String parentProbeType;
        private final String childProbeType;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        MissingDerivedTargetEntityTypeScope(
            @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull String parentProbeType,
            @Nonnull String childProbeType,
            @Nonnull EntityType entityType,
            @Nonnull ProbeStore probeStore,
            @Nonnull TargetStore targetStore) {
            super(topologyGraph);
            this.parentProbeType = Objects.requireNonNull(parentProbeType);
            this.childProbeType = Objects.requireNonNull(childProbeType);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<TopologyEntity> entities() {
            // Make a set of targets of the parent probe that don't have derived targets of the
            // child probe type
            Set<Long> targetIdsMissingDerivedTarget =
                targetStore.getProbeTargets(probeStore
                    .getProbeIdForType(parentProbeType).orElse(-1L))
                    .stream()
                    .filter(target -> !targetStore.getDerivedTargetIds(target.getId()).stream()
                        .map(targetId -> targetStore.getTarget(targetId).orElse(null))
                        .filter(Objects::nonNull)
                        .anyMatch(derivedTarget -> childProbeType.equals(
                            derivedTarget.getProbeInfo().getProbeType())))
                    .map(target -> target.getId())
                .collect(Collectors.toSet());

            return getTopologyGraph().entitiesOfType(entityType)
                .filter(TopologyEntity::hasDiscoveryOrigin)
                .filter(entity -> entity.getDiscoveryOrigin().get()
                                .getDiscoveredTargetDataMap().keySet().stream()
                                .anyMatch(targetIdsMissingDerivedTarget::contains));
        }
    }
}
