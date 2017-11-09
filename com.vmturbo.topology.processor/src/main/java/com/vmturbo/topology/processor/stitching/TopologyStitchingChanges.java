package com.vmturbo.topology.processor.stitching;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperationResult.StitchingChange;

/**
 * A collection of objects representing the changes that can be made to a topology during stitching.
 */
public class TopologyStitchingChanges {
    private TopologyStitchingChanges() {
        // Never instantiate this class. Only inner classes may be instantiated.s
    }

    /**
     * Represents the removal of an individual {@link StitchingEntity} from the eventual topology.
     */
    @Immutable
    public static class RemoveEntityChange implements StitchingChange {
        private final StitchingEntity entityToRemove;
        private final StitchingContext stitchingContext;

        public RemoveEntityChange(@Nonnull final StitchingContext stitchingContext,
                                  @Nonnull final StitchingEntity entityToRemove) {
            this.entityToRemove = Objects.requireNonNull(entityToRemove);
            this.stitchingContext = Objects.requireNonNull(stitchingContext);
        }

        @Override
        public void applyChange() {
            Preconditions.checkArgument(entityToRemove instanceof TopologyStitchingEntity);
            final TopologyStitchingEntity removed = (TopologyStitchingEntity)entityToRemove;
            stitchingContext.removeEntity(removed);
        }
    }

    /**
     * Represents the update of relationships of an individual {@link StitchingEntity} in
     * the eventual topology based on the changes to the commodities bought.
     *
     * We do NOT currently support destructive changes to commodities sold.
     */
    @Immutable
    public static class UpdateEntityRelationshipsChange implements StitchingChange {
        private final StitchingEntity entityToUpdate;
        private final Consumer<StitchingEntity> updateMethod;

        public UpdateEntityRelationshipsChange(@Nonnull final StitchingEntity entityToUpdate,
                                               @Nonnull final Consumer<StitchingEntity> updateMethod) {
            this.entityToUpdate = entityToUpdate;
            this.updateMethod = updateMethod;
        }

        @Override
        public void applyChange() {
            Preconditions.checkArgument(entityToUpdate instanceof TopologyStitchingEntity);

            // Track providers before and after applying the update.
            final List<StitchingEntity> providersBeforeChangeCopy = entityToUpdate.getProviders().stream()
                .collect(Collectors.toList());
            updateMethod.accept(entityToUpdate);
            final Set<StitchingEntity> providersAfterChange = entityToUpdate.getProviders();

            // All removed providers should no longer relate to the destination through a consumer relationship.
            providersBeforeChangeCopy.stream()
                .filter(provider -> !providersAfterChange.contains(provider))
                .forEach(provider -> ((TopologyStitchingEntity)provider).removeConsumer(entityToUpdate));
            // All added providers should now relate to the destination through a consumer relationship.
            providersAfterChange.stream()
                .filter(provider -> !providersBeforeChangeCopy.contains(provider))
                .forEach(provider -> ((TopologyStitchingEntity)provider).addConsumer(entityToUpdate));
        }
    }

    /**
     * A stitching change that makes no changes to relationships on any entity in the topology.
     * This sort of change may update the builder or the values in some commodity on a single entity.
     */
    public static class UpdateEntityAloneChange implements StitchingChange {
        private final StitchingEntity entityToUpdate;
        private final Consumer<StitchingEntity> updateMethod;

        public UpdateEntityAloneChange(@Nonnull final StitchingEntity entityToUpdate,
                                       @Nonnull final Consumer<StitchingEntity> updateMethod) {
            this.entityToUpdate = Objects.requireNonNull(entityToUpdate);
            this.updateMethod = Objects.requireNonNull(updateMethod);
        }

        @Override
        public void applyChange() {
            updateMethod.accept(entityToUpdate);
        }
    }
}
