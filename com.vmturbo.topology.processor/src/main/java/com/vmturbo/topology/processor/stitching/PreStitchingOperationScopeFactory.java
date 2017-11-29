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
import com.vmturbo.stitching.PreStitchingOperation.CalculationScope;
import com.vmturbo.stitching.PreStitchingOperation.CalculationScopeFactory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingResult.Builder;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A Factory for constructing concrete {@link CalculationScope}s for use in
 * {@link PreStitchingOperation}s.
 *
 * These scopes determine which entities in the {@link StitchingContext} are fed to the
 * {@link PreStitchingOperation#performOperation(Stream, Builder)}
 * method.
 */
public class PreStitchingOperationScopeFactory implements CalculationScopeFactory {

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
    public CalculationScope globalScope() {
        return new GlobalCalculationScope(stitchingContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CalculationScope probeScope(@Nonnull final String probeTypeName) {
        return new ProbeTypeCalculationScope(stitchingContext, probeTypeName, probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CalculationScope entityTypeScope(@Nonnull final EntityType entityType) {
        return new EntityTypeCalculationScope(stitchingContext, entityType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CalculationScope probeEntityTypeScope(@Nonnull final String probeTypeName,
                                                 @Nonnull final EntityType entityType) {
        return new ProbeEntityTypeCalculationScope(stitchingContext, probeTypeName, entityType,
            probeStore, targetStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CalculationScope probeCategoryEntityTypeScope(@Nonnull final ProbeCategory probeCategory,
                                                         @Nonnull final EntityType entityType) {
        return new ProbeCategoryEntityTypeCalculationScope(stitchingContext, probeCategory, entityType,
            probeStore, targetStore);
    }

    public StitchingContext getStitchingContext() {
        return stitchingContext;
    }

    /**
     * The base class for calculation scopes. Takes a {@link StitchingContext}.
     */
    private static abstract class BaseCalculationScope implements CalculationScope {
        private final StitchingContext stitchingContext;

        public BaseCalculationScope(@Nonnull final StitchingContext stitchingContext) {
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
        }

        public StitchingContext getStitchingContext() {
            return stitchingContext;
        }
    }

    /**
     * A calculation scope for applying a calculation globally to all entities.
     */
    private static class GlobalCalculationScope extends BaseCalculationScope {
        public GlobalCalculationScope(@Nonnull StitchingContext stitchingContext) {
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
    private static class ProbeTypeCalculationScope extends BaseCalculationScope {

        private final String probeTypeName;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeTypeCalculationScope(@Nonnull StitchingContext stitchingContext,
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
    private static class EntityTypeCalculationScope extends BaseCalculationScope {

        private final EntityType entityType;

        public EntityTypeCalculationScope(@Nonnull StitchingContext stitchingContext,
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
    private static class ProbeEntityTypeCalculationScope extends BaseCalculationScope {

        private final String probeTypeName;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeEntityTypeCalculationScope(@Nonnull StitchingContext stitchingContext,
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
    private static class ProbeCategoryEntityTypeCalculationScope extends BaseCalculationScope {

        private final ProbeCategory probeCategory;
        private final EntityType entityType;
        private final ProbeStore probeStore;
        private final TargetStore targetStore;

        public ProbeCategoryEntityTypeCalculationScope(@Nonnull StitchingContext stitchingContext,
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
