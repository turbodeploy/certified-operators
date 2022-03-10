package com.vmturbo.topology.processor.stitching;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingChanges.UpdateEntityAloneChange;

/**
 * A builder for stitching results with concrete implementations to instantiate change
 * objects that can be used to mutate the topology during stitching. These changes are allowed
 * in the post-stitching phase. The equivalent for pre-stitching and main stitching is
 * {@link StitchingResultBuilder}.
 *
 * Provides implementations for the various methods to queue changes onto a {@link TopologicalChangelog}.
 */
public class PostStitchingResultBuilder extends EntityChangesBuilder<TopologyEntity> {
    /**
     * Create a new {@link PostStitchingResultBuilder}.
     */
    public PostStitchingResultBuilder() {
    }

    @Override
    public TopologicalChangelog build() {
        return buildInternal();
    }

    @Override
    public EntityChangesBuilder<TopologyEntity> queueUpdateEntityAlone(@Nonnull TopologyEntity entityToUpdate,
                                                                   @Nonnull Consumer<TopologyEntity> updateMethod) {
        changes.add(new UpdateEntityAloneChange<>(entityToUpdate, updateMethod));

        return this;
    }
}
