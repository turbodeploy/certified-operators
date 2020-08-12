package com.vmturbo.topology.processor.stitching;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A Factory for constructing concrete {@link StitchingScope}s for use in
 * {@link PreStitchingOperation}s and {@link com.vmturbo.stitching.StitchingOperation}s.
 *
 * These scopes determine which entities in the {@link StitchingContext} are fed to the
 * {@link PreStitchingOperation#performOperation(Stream, StitchingChangesBuilder)} and
 * {@link com.vmturbo.stitching.StitchingOperation#stitch(Collection, StitchingChangesBuilder)}
 * methods.
 */
public class StitchingOperationScopeFactory implements StitchingScopeFactory<StitchingEntity> {

    private static final Logger logger = LogManager.getLogger();
    private final StitchingContext stitchingContext;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    public StitchingOperationScopeFactory(@Nonnull final StitchingContext stitchingContext,
                                          @Nonnull final ProbeStore probeStore,
                                          @Nonnull final TargetStore targetStore) {
        this.stitchingContext = Objects.requireNonNull(stitchingContext);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> globalScope() {
        return new GlobalStitchingScope(stitchingContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> probeScope(@Nonnull final String probeTypeName) {
        return new ProbeTypeStitchingScope(stitchingContext, probeTypeName, probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> entityTypeScope(@Nonnull final EntityType entityType) {
        return new EntityTypeStitchingScope(stitchingContext, entityType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> multiEntityTypesScope(@Nonnull final List<EntityType> entityTypes) {
        return new MultiEntityTypesStitchingScope(stitchingContext, entityTypes);
    }

    @Override
    public StitchingScope<StitchingEntity> containsAllEntityTypesScope(@Nonnull final List<EntityType> entityTypes) {
        return new ContainsAllEntityTypesStitchingScope(stitchingContext, entityTypes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> probeEntityTypeScope(@Nonnull final String probeTypeName,
                                                 @Nonnull final EntityType entityType) {
        return new ProbeEntityTypeStitchingScope(stitchingContext, probeTypeName, entityType,
            probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> multiProbeEntityTypeScope(@Nonnull final Set<String> probeTypeNames,
                                                                @Nonnull final EntityType entityType) {
        return new MultiProbeEntityTypeStitchingScope(stitchingContext, probeTypeNames, entityType,
                probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> probeCategoryEntityTypeScope(@Nonnull final ProbeCategory probeCategory,
                                                         @Nonnull final EntityType entityType) {
        return new ProbeCategoryEntityTypeStitchingScope(stitchingContext, probeCategory, entityType,
            probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> probeCategoryScope(@Nonnull final ProbeCategory probeCategory) {
        return new ProbeCategoryScope(stitchingContext, probeCategory, probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StitchingScope<StitchingEntity> multiProbeCategoryEntityTypeScope(
            @Nonnull final Set<ProbeCategory> probeCategories,
            @Nonnull final EntityType entityType) {
        return new MultiProbeCategoryEntityTypeStitchingScope(stitchingContext, probeCategories,
                entityType, probeStore, targetStore);
    }

    @Override
    public StitchingScope<StitchingEntity> missingDerivedTargetEntityTypeScope(
        @Nonnull final String parentProbeType,
        @Nonnull final String childProbeType,
        @Nonnull final EntityType entityType) {
        // TODO there is no compelling reason to implement this here right now.  It is very specfic
        // to shared storage wasted files which are only handled in post stitching.  This can be
        // implemented at a later date in the unlikely event we have a use for it.
        throw new UnsupportedOperationException(
            "missingDerivedTargetEntityTypeScope is not supported for stitching or pre-stitching");
    }

    @Override
    public StitchingScope<StitchingEntity> hasAndLacksProbeCategoryEntityTypeStitchingScope(
            @Nonnull Set<ProbeCategory> owningProbeCategories, @Nonnull Set<ProbeCategory> missingProbeCategories, @Nonnull EntityType entityType) {
        throw new UnsupportedOperationException(
                "missingSingleProbeCategoryEntityTypeStitchingScope is not needed for stitching");
    }

    @Override
    public StitchingScope<StitchingEntity> parentTargetEntityType(@Nonnull EntityType entityType,
            long targetId) {
        return new ParentTargetEntityTypeStitchingScope(stitchingContext, entityType, targetId,
                targetStore);
    }

    public StitchingContext getStitchingContext() {
        return stitchingContext;
    }

    /**
     * The base class for calculation scopes. Takes a {@link StitchingContext}.
     */
    private static abstract class BaseStitchingScope implements StitchingScope<StitchingEntity> {
        private final StitchingContext stitchingContext;

        public BaseStitchingScope(@Nonnull final StitchingContext stitchingContext) {
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
        }

        public StitchingContext getStitchingContext() {
            return stitchingContext;
        }
    }

    /**
     * A calculation scope for applying a calculation globally to all entities.
     */
    private static class GlobalStitchingScope extends BaseStitchingScope {
        public GlobalStitchingScope(@Nonnull StitchingContext stitchingContext) {
            super(stitchingContext);
        }

        @Override
        @Nonnull
        public Stream<StitchingEntity> entities() {
            return getStitchingContext().getStitchingGraph().entities().map(Function.identity());
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a specific type of probe.
     */
    private static class ProbeTypeStitchingScope extends BaseStitchingScope {

        private final String probeTypeName;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeTypeStitchingScope(@Nonnull StitchingContext stitchingContext,
                                       @Nonnull final String probeTypeName,
                                       @Nonnull final ProbeStore probeStore,
                                       @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.probeTypeName = Objects.requireNonNull(probeTypeName);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            final Optional<Long> optionalProbeId = probeStore.getProbeIdForType(probeTypeName);
            if (!optionalProbeId.isPresent()) {
                logger.warn("Unable to retrieve entities for " + probeTypeName +
                    " because no probe of that type is currently registered.");
                return Stream.empty();
            }

            final long probeId = optionalProbeId.get();
            return targetStore.getProbeTargets(probeId).stream()
                .map(Target::getId)
                .flatMap(targetId -> getStitchingContext().internalEntities(targetId));
        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific {@link EntityType}.
     */
    private static class EntityTypeStitchingScope extends BaseStitchingScope {

        private final EntityType entityType;

        public EntityTypeStitchingScope(@Nonnull StitchingContext stitchingContext,
                                        @Nonnull final EntityType entityType) {
            super(stitchingContext);
            this.entityType = Objects.requireNonNull(entityType);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            return getStitchingContext().getEntitiesOfType(entityType).map(Function.identity());
        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific {@link EntityType}.
     */
    private static class MultiEntityTypesStitchingScope extends BaseStitchingScope {

        private final List<EntityType> entityTypes;

        public MultiEntityTypesStitchingScope(@Nonnull StitchingContext stitchingContext,
                                              @Nonnull final List<EntityType> entityTypes) {
            super(stitchingContext);
            this.entityTypes = Objects.requireNonNull(entityTypes);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            return entityTypes
                    .stream()
                    .flatMap(entityType -> getStitchingContext().getEntitiesOfType(entityType));
        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific {@link EntityType}.
     */
    private static class ContainsAllEntityTypesStitchingScope extends BaseStitchingScope {

        private final List<EntityType> entityTypes;

        public ContainsAllEntityTypesStitchingScope(@Nonnull StitchingContext stitchingContext,
                @Nonnull final List<EntityType> entityTypes) {
            super(stitchingContext);
            this.entityTypes = Objects.requireNonNull(entityTypes);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            // return empty stream if entities for any EntityType are not available
            if (entityTypes.stream().anyMatch(entityType -> !getStitchingContext()
                    .getEntitiesOfType(entityType).findAny().isPresent())) {
                return Stream.empty();
            }

            return entityTypes.stream().flatMap(entityType ->
                    getStitchingContext().getEntitiesOfType(entityType));
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

        public ProbeEntityTypeStitchingScope(@Nonnull StitchingContext stitchingContext,
                                             @Nonnull final String probeTypeName,
                                             @Nonnull final EntityType entityType,
                                             @Nonnull final ProbeStore probeStore,
                                             @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.probeTypeName = Objects.requireNonNull(probeTypeName);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            final Optional<Long> optionalProbeId = probeStore.getProbeIdForType(probeTypeName);
            if (!optionalProbeId.isPresent()) {
                logger.debug("Unable to retrieve entities for " + probeTypeName +
                    " because no probe of that type is currently registered.");
                return Stream.empty();
            }

            final long probeId = optionalProbeId.get();
            return targetStore.getProbeTargets(probeId).stream()
                .map(Target::getId)
                .flatMap(targetId -> getStitchingContext().internalEntities(entityType, targetId));
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

        public MultiProbeEntityTypeStitchingScope(@Nonnull StitchingContext stitchingContext,
                                             @Nonnull final Set<String> probeTypeNames,
                                             @Nonnull final EntityType entityType,
                                             @Nonnull final ProbeStore probeStore,
                                             @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.probeTypeNames = Objects.requireNonNull(probeTypeNames);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {

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

            return probeTargetIds.stream()
                    .flatMap((targetId -> getStitchingContext().internalEntities(entityType, targetId)));
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

        public ProbeCategoryEntityTypeStitchingScope(@Nonnull StitchingContext stitchingContext,
                                                     @Nonnull final ProbeCategory probeCategory,
                                                     @Nonnull final EntityType entityType,
                                                     @Nonnull final ProbeStore probeStore,
                                                     @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.probeCategory = Objects.requireNonNull(probeCategory);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            final List<Long> probeIdsForCategory = probeStore.getProbeIdsForCategory(probeCategory);

            return probeIdsForCategory.stream()
                .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream())
                .map(Target::getId)
                .flatMap(targetId -> getStitchingContext().internalEntities(entityType, targetId));
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a probe category.
     */
    private static class ProbeCategoryScope extends BaseStitchingScope {

        private final ProbeCategory probeCategory;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        ProbeCategoryScope(@Nonnull StitchingContext stitchingContext,
                           @Nonnull final ProbeCategory probeCategory,
                           @Nonnull final ProbeStore probeStore,
                           @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.probeCategory = Objects.requireNonNull(probeCategory);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            final Set<Long> probeCategoryTargetIds = probeStore.getProbeIdsForCategory(probeCategory).stream()
                .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream()
                    .map(Target::getId))
                .collect(Collectors.toSet());

            return getStitchingContext().internalEntities(probeCategoryTargetIds)
                .map(Function.identity());
        }
    }

    /**
     * A calculation scope for applying a calculation to entities discovered by a set of probe
     * categories with a specific {@link EntityType}.
     */
    private static class MultiProbeCategoryEntityTypeStitchingScope extends BaseStitchingScope {

        private final Set<ProbeCategory> probeCategories;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public MultiProbeCategoryEntityTypeStitchingScope(
                @Nonnull StitchingContext stitchingContext,
                @Nonnull final Set<ProbeCategory> probeCategories,
                @Nonnull final EntityType entityType,
                @Nonnull final ProbeStore probeStore,
                @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.probeCategories = Objects.requireNonNull(probeCategories);
            this.entityType = Objects.requireNonNull(entityType);
            this.probeStore = Objects.requireNonNull(probeStore);
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            return probeCategories.stream()
                    .flatMap(category -> probeStore.getProbeIdsForCategory(category).stream())
                    .flatMap(probeId -> targetStore.getProbeTargets(probeId).stream())
                    .map(Target::getId)
                    .flatMap(targetId -> getStitchingContext().internalEntities(entityType, targetId));
        }
    }

    /**
     * A calculation scope for applying a calculation globally to entities of a specific
     * {@link EntityType} that come from a parent target with a given target id.
     */
    private static class ParentTargetEntityTypeStitchingScope extends BaseStitchingScope {

        private final EntityType entityType;

        private final long targetId;

        private final TargetStore targetStore;

        ParentTargetEntityTypeStitchingScope(@Nonnull StitchingContext stitchingContext,
                @Nonnull final EntityType entityType, long targetId,
                @Nonnull final TargetStore targetStore) {
            super(stitchingContext);
            this.entityType = Objects.requireNonNull(entityType);
            this.targetId = targetId;
            this.targetStore = Objects.requireNonNull(targetStore);
        }

        @Nonnull
        @Override
        public Stream<StitchingEntity> entities() {
            final Set<Long> parentIds = targetStore.getParentTargetIds(targetId);
            logger.trace("Returning entities discovered by parents {}.", () -> parentIds.stream()
                    .map(targetStore::getTarget)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(Target::toString)
                    .collect(Collectors.toSet()));
            return getStitchingContext().getEntitiesOfType(entityType)
                    .filter(entity -> entity.getDiscoveringTargetIds()
                            .anyMatch(parentIds::contains))
                    .map(Function.identity());
        }
    }
}
