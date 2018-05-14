package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.EntityScopeFilters;

/**
 * When adding multiple Hyper-V targets (without VMM) in the same cluster using the
 * non-cluster option, each Hyper-V will provide information about the same datacenter.
 *
 * This results in multiple of the same datacenter being discovered. This pre-stitching
 * operation merges those multiple datacenters down to a single instance.
 *
 * Note that since the values on the datacenter are made up in any case, the merger
 * does not have to do much work.
 */
public class MergeSharedDatacentersPreStitchingOperation implements PreStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
        @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeEntityTypeScope(SDKProbeType.HYPERV.getProbeType(), EntityType.DATACENTER);
    }

    @Nonnull
    @Override
    public TopologicalChangelog performOperation(
        @Nonnull Stream<StitchingEntity> entities,
        @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        EntityScopeFilters.sharedEntitiesByOid(entities).forEach(sharedDatacenters ->
            mergeSharedDatacenters(sharedDatacenters, resultBuilder));

        return resultBuilder.build();
    }

    private void mergeSharedDatacenters(@Nonnull final List<StitchingEntity> sharedDatacenterInstances,
                                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        if (sharedDatacenterInstances.size() > 1) {
            // All the datacenters should be mostly identical so which one we pick does not really matter.
            final StitchingEntity baseInstance = Objects.requireNonNull(sharedDatacenterInstances.remove(0));

            // Merge all other instances onto the first one.
            sharedDatacenterInstances.forEach(otherInstance -> {
                if (!baseInstance.getEntityBuilder().build().equals(otherInstance.getEntityBuilder().build())) {
                    logger.warn("Hyper-V datacenters {} and {} are not equal despite having the same OID.",
                        baseInstance, otherInstance);
                }

                resultBuilder.queueEntityMerger(mergeEntity(otherInstance)
                .onto(baseInstance)
                // Keep the displayName for the entity alphabetically first to prevent ping-ponging
                .addFieldMerger(EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST));
            });
        }
    }
}
