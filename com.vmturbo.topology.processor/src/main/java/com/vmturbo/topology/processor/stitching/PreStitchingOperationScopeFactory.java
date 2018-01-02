package com.vmturbo.topology.processor.stitching;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A Factory for constructing concrete {@link StitchingScope}s for use in
 * {@link PreStitchingOperation}s.
 *
 * These scopes determine which entities in the {@link StitchingContext} are fed to the
 * {@link PreStitchingOperation#performOperation(Stream, StitchingChangesBuilder)}
 * method.
 */
public class PreStitchingOperationScopeFactory implements StitchingScopeFactory<StitchingEntity> {

    private static final Logger logger = LogManager.getLogger();
    private final StitchingContext stitchingContext;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;

    public PreStitchingOperationScopeFactory(@Nonnull final StitchingContext stitchingContext,
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
    public StitchingScope<StitchingEntity> probeEntityTypeScope(@Nonnull final String probeTypeName,
                                                 @Nonnull final EntityType entityType) {
        return new ProbeEntityTypeStitchingScope(stitchingContext, probeTypeName, entityType,
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
}
