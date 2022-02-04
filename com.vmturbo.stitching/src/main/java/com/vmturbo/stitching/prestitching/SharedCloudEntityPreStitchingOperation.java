package com.vmturbo.stitching.prestitching;

import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MergePropertiesStrategy;
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

    private final MergePropertiesStrategy mergePropertiesStrategy;

    /**
     * Constructor.
     * @param scopeGetter function to get the scope for this pre stitching operation
     *                    associated with custom identity functions.
     * @param mergeProperties whether to combine the entity properties
     *
     */
    public SharedCloudEntityPreStitchingOperation(
            Function<StitchingScopeFactory<StitchingEntity>, StitchingScope<StitchingEntity>> scopeGetter,
            boolean mergeProperties) {
        super(scopeGetter);
        this.mergePropertiesStrategy = mergeProperties
                ? MergePropertiesStrategy.MERGE_IF_NOT_PRESENT : MergePropertiesStrategy.MERGE_NOTHING;
    }

    @Override
    protected void mergeSharedEntities(
            @Nonnull List<StitchingEntity> sources,
            @Nonnull StitchingEntity destination,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        resultBuilder.queueEntityMerger(MergeEntities.mergeEntities(sources)
                .onto(destination,
                        // do not merge commodities for shared cloud entities since they are all the same
                        mergePropertiesStrategy, false));
    }
}
