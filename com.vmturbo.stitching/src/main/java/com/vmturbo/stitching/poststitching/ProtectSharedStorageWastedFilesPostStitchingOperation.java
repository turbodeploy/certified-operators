package com.vmturbo.stitching.poststitching;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.utilities.WastedFiles;

/**
 * Prevent removal of files that we aren't sure are wasted. We remove files from associated
 * wasted volume of the storages that are missing at least one derived storage browsing target for
 * the corresponding main target. This protects files that are on shared VC Storages where one or
 * more VC targets of the shared storage have storage browsing enabled and one or more do not.
 * In this case, we may not have discovered some files that reside on VMs in the targets that
 * don't have storage browsing enabled. This will avoid creating delete actions for those files.
 */
public class ProtectSharedStorageWastedFilesPostStitchingOperation
    implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
        @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        // scope to only shared storages that have at least one VC target with storage browsing
        // enabled and at least one VC target with storage browsing disabled
        return stitchingScopeFactory.missingDerivedTargetEntityTypeScope(
            SDKProbeType.VCENTER.getProbeType(),
            SDKProbeType.VC_STORAGE_BROWSE.getProbeType(),
            EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
        @Nonnull final Stream<TopologyEntity> entities,
        @Nonnull final EntitySettingsCollection settingsCollection,
        @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        final Set<TopologyEntity> storagesBrowsingDisabled = new HashSet<>();
        // for each storage find the associated wasted files volume and delete all files on it.
        // The scope will ensure that only shared storages that we can't definitively identify
        // wasted files on are passed in.
        entities.forEach(storage -> {
            WastedFiles.getWastedFilesVirtualVolume(storage).ifPresent(wastedFilesVolume -> {
                // Just for logging purposes, collect a set of storages for which we will be
                // removing wasted files from their wasted files volumes
                storagesBrowsingDisabled.add(storage);
                resultBuilder.queueUpdateEntityAlone(wastedFilesVolume, toUpdate ->
                        toUpdate.getTopologyEntityDtoBuilder()
                                .getTypeSpecificInfoBuilder()
                                .getVirtualVolumeBuilder()
                                .clearFiles());
            });
        });
        if (!storagesBrowsingDisabled.isEmpty()) {
            logger.info("No delete wasted file actions will be generated for {} storages "
                    + "since one or more of their discovering targets has storage "
                    + "browsing disabled.",
                () -> storagesBrowsingDisabled.size());
            logger.debug("Ignoring wasted files for the following datastores: {}",
                () -> storagesBrowsingDisabled.stream()
                    .map(TopologyEntity::getDisplayName)
                    .collect(Collectors.joining(", ")));
        }
        return resultBuilder.build();
    }
}
