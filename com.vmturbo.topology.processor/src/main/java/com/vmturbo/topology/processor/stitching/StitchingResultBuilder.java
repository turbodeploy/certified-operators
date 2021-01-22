package com.vmturbo.topology.processor.stitching;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.EntityToAdd;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.MergeEntities.MergeEntitiesDetails;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.AddEntitiesChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.MergeEntitiesChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.RemoveEntityChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityAloneChange;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityRelationshipsChange;

/**
 * A builder for stitching results with concrete implementations to instantiate change
 * objects that can be used to mutate the topology during stitching. These changes are allowed
 * in the pre-stitching and main stitching phases. The equivalent for post-stitching is
 * {@link PostStitchingResultBuilder}.
 *
 * Provides implementations for the various methods to queue changes onto a {@link TopologicalChangelog}.
 */
public class StitchingResultBuilder extends StitchingChangesBuilder<StitchingEntity> {
    private final StitchingContext stitchingContext;

    /**
     * Create a new {@link StitchingResultBuilder} for a given {@link StitchingContext}.
     *
     * @param stitchingContext The {@link StitchingContext} to associate with this builder.
     *                         This context will be used by some of the changes that can be queued
     *                         via this builder.
     */
    public StitchingResultBuilder(@Nonnull final StitchingContext stitchingContext) {
        this.stitchingContext = Objects.requireNonNull(stitchingContext);
    }

    @Override
    public TopologicalChangelog build() {
        return buildInternal();
    }

    @Override
    public StitchingChangesBuilder<StitchingEntity> queueEntityRemoval(@Nonnull StitchingEntity entity) {
        changes.add(new RemoveEntityChange(stitchingContext, entity));
        return this;
    }

    @Override
    public StitchingChangesBuilder<StitchingEntity> queueEntitiesAddition(@Nonnull final List<EntityToAdd> entities) {
        changes.add(new AddEntitiesChange(stitchingContext, entities));
        return this;
    }

    @Override
    public StitchingChangesBuilder<StitchingEntity> queueEntityMerger(@Nonnull MergeEntitiesDetails details) {
        changes.add(new MergeEntitiesChange(stitchingContext, details));

        return this;
    }

    @Override
    public StitchingChangesBuilder<StitchingEntity> queueChangeRelationships(
        @Nonnull final StitchingEntity entityToUpdate, @Nonnull final Consumer<StitchingEntity> updateMethod) {
        changes.add(new UpdateEntityRelationshipsChange(entityToUpdate, updateMethod));

        return this;
    }

    @Override
    public StitchingChangesBuilder<StitchingEntity> queueUpdateEntityAlone(
        @Nonnull StitchingEntity entityToUpdate, @Nonnull Consumer<StitchingEntity> updateMethod) {
        changes.add(new UpdateEntityAloneChange<>(entityToUpdate, updateMethod));

        return this;
    }
}
