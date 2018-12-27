package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.EntityScopeFilters;

/**
 * Prestitching operation that merges VirtualVolumes that are shared by more than one VC
 * Storage Browsing Target.  We only care about preserving the files from the most current
 * Virtual Volume and the connected relationships for all of the shared volumes get merged onto
 * the most current.
 */
public class SharedVirtualVolumePreStitchingOperation implements PreStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        // Apply this calculation to all VirtualVolumes discovered by storage browsing probes.
        return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.STORAGE_BROWSING,
                EntityType.VIRTUAL_VOLUME);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(@Nonnull Stream<StitchingEntity> entities,
                                                                  @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        EntityScopeFilters.sharedEntitiesByOid(entities).forEach(sharedVolumes ->
                mergeSharedVolumes(sharedVolumes, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Merge multiple instances of the shared virtual volume down to a single instance.
     *
     * We retain the most recently discovered volume since it will have the most up to date file
     * list.  The only thing we care about from the other volumes is the connected relationships
     * which get merged onto the most recently discovered volume.
     *
     * @param sharedVolumeInstances The shared volumes. Note that there must be at least 2 volumes in the group
     *                               of shared volumes.
     * @param resultBuilder The builder for the result of the stitching calculation.
     */
    private void mergeSharedVolumes(@Nonnull final List<StitchingEntity> sharedVolumeInstances,
                                    @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        Preconditions.checkArgument(sharedVolumeInstances.size() > 1, "There must be multiple instances of a " +
                "shared volume.");
        logger.info("Merging shared volumes: {}", sharedVolumeInstances);

        // Find the most recently updated shared volume.
        final StitchingEntity mostUpToDate = sharedVolumeInstances.stream()
                .sorted((a, b) -> Long.compare(b.getLastUpdatedTime(), a.getLastUpdatedTime())) // Sort descending
                .findFirst()
                .get();

        // Remove the duplicate instances of this object after merging connectedTo and connectedFrom
        // because we now have all the data we require from them.
        sharedVolumeInstances.stream()
                .filter(volumeInstance -> volumeInstance != mostUpToDate)
                .forEach(duplicateInstance ->
                        mergeVolumes(duplicateInstance, mostUpToDate, resultBuilder));
    }

    /**
     * Merge the connectedTo/connectedFrom relationships from the duplicate onto the mostUpToDate
     * and take the lexicographically first displayname, so that displayname does not change based
     * on discovery order.
     *
     * @param duplicateInstance virtual volume to be merged onto mostUpToDateInstance
     * @param mostUpToDateInstance the last discovered instance of the virtual volume
     * @param resultBuilder builder to queue the change onto.
     */
    private void mergeVolumes(@Nonnull final StitchingEntity duplicateInstance,
                                               @Nonnull final StitchingEntity mostUpToDateInstance,
                                               @Nonnull final StitchingChangesBuilder resultBuilder) {
        resultBuilder.queueEntityMerger(mergeEntity(duplicateInstance)
                .onto(mostUpToDateInstance)
                // Keep the displayName for the entity alphabetically first to prevent ping-ponging
                .addFieldMerger(EntityFieldMergers.DISPLAY_NAME_LEXICOGRAPHICALLY_FIRST));
    }
}
