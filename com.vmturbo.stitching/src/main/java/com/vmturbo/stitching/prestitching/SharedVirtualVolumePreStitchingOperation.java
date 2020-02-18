package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityFieldMergers;

/**
 * Prestitching operation that merges VirtualVolumes that are shared by more than one VC
 * Storage Browsing Target.  The only VirtualVolumes shared among more than one storage browsing
 * target are those representing wasted files in VC.  For these wasted storage volumes, the only
 * files that are truly wasted are those that appear on all shared wasted storage volumes.  So we
 * remove any files that do not appear on all shared volumes.
 */
public class SharedVirtualVolumePreStitchingOperation extends
    SharedEntityDefaultPreStitchingOperation {

    public SharedVirtualVolumePreStitchingOperation() {
        super(stitchingScopeFactory -> stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.STORAGE_BROWSING,
            EntityType.VIRTUAL_VOLUME));
    }

    @Override
    protected void mergeSharedEntities(@Nonnull final StitchingEntity source,
                                       @Nonnull final StitchingEntity destination,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        resultBuilder.queueEntityMerger(mergeEntity(source)
            .onto(destination)
            // Keep the displayName for the entity alphabetically first to prevent ping-ponging
            .addFieldMerger(EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST)
            .addFieldMerger(EntityFieldMergers.VIRTUAL_VOLUME_FILELIST_INTERSECTION));
    }
}
