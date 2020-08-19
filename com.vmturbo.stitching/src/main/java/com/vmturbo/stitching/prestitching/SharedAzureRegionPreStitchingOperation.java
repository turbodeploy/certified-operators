package com.vmturbo.stitching.prestitching;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.MergeEntities;
import com.vmturbo.stitching.utilities.MergePropertiesStrategy;

/**
 * Pre-stitching operation to merge Azure Regions that are shared by more than one Azure
 * Subscription target.
 * Azure Subscription targets send CPU quota information in entity properties of {@code EntityDTO}s
 * representing Regions. Since Regions are shared by Azure targets and each target sends quotas for
 * one subscription only, we need to merge properties of Region entities so that resulting entity
 * represent quotas for all subscriptions.
 */
public class SharedAzureRegionPreStitchingOperation extends
        SharedEntityDefaultPreStitchingOperation {

    /**
     * Constructs an instance of {@code SharedAzureRegionPreStitchingOperation}.
     */
    public SharedAzureRegionPreStitchingOperation() {
        super(stitchingScopeFactory -> stitchingScopeFactory.probeEntityTypeScope(
                SDKProbeType.AZURE.getProbeType(), EntityType.REGION));
    }

    @Override
    protected void mergeSharedEntities(@Nonnull final StitchingEntity source,
                                       @Nonnull final StitchingEntity destination,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        resultBuilder.queueEntityMerger(MergeEntities.mergeEntity(source)
                .onto(destination, MergePropertiesStrategy.JOIN));
    }
}
