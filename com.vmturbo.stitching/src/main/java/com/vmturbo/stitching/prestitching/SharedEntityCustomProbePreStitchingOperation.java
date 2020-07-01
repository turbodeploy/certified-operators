package com.vmturbo.stitching.prestitching;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.MergeEntities;

/**
 * Custom probes such as DIF (Data Ingestion Framework) and UDT (User Defined Topology).
 * <p/>
 * These multiple instances will share the same OID and should be merged down into a single combined instance of
 * the entity. In general, the merging attempts to preserve the values from the most recently discovered instance
 * of this entity over other values.
 */
public class SharedEntityCustomProbePreStitchingOperation extends SharedEntityDefaultPreStitchingOperation {
    public SharedEntityCustomProbePreStitchingOperation() {
        super(scopeFactory -> scopeFactory.probeCategoryScope(ProbeCategory.CUSTOM));
    }

    /**
     * Merge entities discovered by multiple Custom probes down to a single entity.
     *
     * @param source The entity we are merging from
     * @param destination The entity we are merging onto
     * @param resultBuilder The builder we use for building the merge.
     */
    @Override
    protected void mergeSharedEntities(@Nonnull StitchingEntity source,
                                       @Nonnull StitchingEntity destination,
                                       @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // In the future we may also need to merge miscellaneous fields.
        // See {@code super#mergeMessageBuilders) but this is a potentially expensive
        // operation we don't need for now.
        resultBuilder.queueChangeRelationships(destination,
            dst -> CopyCommodities.copyCommodities().from(source).to(dst));
        resultBuilder.queueEntityMerger(MergeEntities.mergeEntity(source).onto(destination)
            // Keep the displayName for the entity alphabetically first to prevent ping-ponging
            .addFieldMerger(EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST));
    }
}
