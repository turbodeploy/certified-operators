package com.vmturbo.stitching.prestitching;

import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.MergeEntities;

/**
 * Merge entities of a particular type with the same OID that are discovered by a given probe type.
 * We assume that all data in the entities is the same and pick an arbitrary one to keep.
 */
public class SharedCloudEntityPreStitchingOperation extends
        SharedEntityDefaultPreStitchingOperation {

    /**
     * Constructor.
     *
     * @param scopeGetter function to get the scope for this pre stitching operation
     * associated with custom identity functions.
     */
    public SharedCloudEntityPreStitchingOperation(
            Function<StitchingScopeFactory<StitchingEntity>, StitchingScope<StitchingEntity>> scopeGetter) {
        super(scopeGetter);
    }

    @Override
    protected void mergeSharedEntities(@Nonnull StitchingEntity source,
            @Nonnull StitchingEntity destination,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        resultBuilder.queueEntityMerger(MergeEntities.mergeEntity(source).onto(destination));
    }
}
