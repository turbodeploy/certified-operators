package com.vmturbo.topology.processor.stitching;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperationResult;
import com.vmturbo.stitching.StitchingOperationResult.Builder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.RemoveEntityChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityAloneChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityRelationshipsChange;

/**
 * A builder for stitching results with concrete implementations to instantiate change
 * objects that can be used to mutate the topology during stitching.
 *
 * Provides implementations for the various methods to queue changes onto a {@link StitchingOperationResult}.
 */
public class TopologyStitchingResultBuilder extends StitchingOperationResult.Builder {
    private final StitchingContext stitchingContext;

    /**
     * Create a new TopologyStitchingResultBuilder for a given {@link StitchingContext}.
     *
     * @param stitchingContext The {@link StitchingContext} to associate with this builder.
     *                         This context will be used by some of the changes that can be queued
     *                         via this builder.
     */
    public TopologyStitchingResultBuilder(@Nonnull final StitchingContext stitchingContext) {
        this.stitchingContext = Objects.requireNonNull(stitchingContext);
    }

    @Override
    public StitchingOperationResult build() {
        return buildInternal();
    }

    @Override
    public Builder queueEntityRemoval(@Nonnull StitchingEntity entity) {
        changes.add(new RemoveEntityChange(stitchingContext, entity));

        return this;
    }

    @Override
    public Builder queueChangeRelationships(@Nonnull final StitchingEntity entityToUpdate,
                                            @Nonnull final Consumer<StitchingEntity> updateMethod) {
        changes.add(new UpdateEntityRelationshipsChange(entityToUpdate, updateMethod));

        return this;
    }

    @Override
    public Builder queueUpdateEntityAlone(@Nonnull StitchingEntity entityToUpdate,
                                          @Nonnull Consumer<StitchingEntity> updateMethod) {
        changes.add(new UpdateEntityAloneChange(entityToUpdate, updateMethod));

        return this;
    }
}
