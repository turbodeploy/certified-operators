package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Find all virtual volumes associated with each storage via the connectedFrom relationship.
 * Identify the wasted files virtual volume by the fact that it is not associated with any virtual
 * machines via connectedFrom.  Generate a list of used files by collecting the files from all
 * virtual volumes that are associated with virtual machines.  Remove the used files from the list
 * of files associated with the wasted files virtual volume so that all the files remaining represent
 * actual wasted files.
 */
public class WastedFilesPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeEntityTypeScope(SDKProbeType.VCENTER.getProbeType(),
                EntityType.STORAGE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        // iterate over storages and update the related wasted files volume for each by removing
        // any file that is used by a VirtualVolume associated with a VirtualMachine
        entities.forEach(storage -> {
            getWastedFilesVirtualVolume(storage).ifPresent(wastedFilesVolume -> {
                Set<VirtualVolumeFileDescriptor> filesUsedByVms = getAllVmVirtualVolumes(storage)
                        .stream()
                        .flatMap(vmVolume -> vmVolume.getTopologyEntityDtoBuilder()
                                .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream())
                        .collect(Collectors.toSet());

                resultBuilder.queueUpdateEntityAlone(wastedFilesVolume, toUpdate -> {
                    Set<VirtualVolumeFileDescriptor> allFiles =
                            Sets.newHashSet(toUpdate.getTopologyEntityDtoBuilder()
                                    .getTypeSpecificInfo()
                                    .getVirtualVolume()
                                    .getFilesList());
                    toUpdate.getTopologyEntityDtoBuilder()
                            .getTypeSpecificInfoBuilder()
                            .getVirtualVolumeBuilder()
                            .clearFiles()
                            .addAllFiles(Sets.difference(allFiles,
                                    filesUsedByVms));
                });
             });
        });

        return resultBuilder.build();
    }

    /**
     * Find the VirtualVolume representing wasted files for a given Storage.  The wasted files
     * VirtualVolume can be identified by the fact that it has no connected VirtualMachines
     *
     * @param storage the Storage whose wasted files VirtualVolume we want.
     * @return Optional containing the wasted files VirtualVolume or else Optional.empty if no such
     * VirtualVolume exists.
     */
    private Optional<TopologyEntity> getWastedFilesVirtualVolume(TopologyEntity storage) {
        List<TopologyEntity> wastedFilesVolumes = storage.getConnectedFromEntities().stream()
                .filter(topoEntity -> topoEntity.getEntityType() ==
                        EntityType.VIRTUAL_VOLUME.getNumber())
                .filter(virtualVolume -> !virtualVolume.getConnectedFromEntities().stream()
                        .map(TopologyEntity::getEntityType)
                        .anyMatch(type -> type == EntityType.VIRTUAL_MACHINE_VALUE))
                .collect(Collectors.toList());
        if (wastedFilesVolumes.isEmpty()) {
            logger.debug("No wasted files volume associated with storage {}", storage.getOid());
            return Optional.empty();
        }
        if (wastedFilesVolumes.size() > 1) {
            logger.warn("Multiple wasted files volumes associated with storage {}. Using {}.",
                    storage.getOid(), wastedFilesVolumes.get(0).getOid());
        }
        return Optional.of(wastedFilesVolumes.get(0));
    }

    /**
     * Find all VirtualVolumes related to the Storage that are also related to VirtualMachines.
     *
     * @param storage {@link TopologyEntity} representing a Storage
     * @return Set of VirtualVolumes associated with that Storage as well as at least one VM
     */
    private Set<TopologyEntity> getAllVmVirtualVolumes(TopologyEntity storage) {
        return storage.getConnectedFromEntities().stream()
                .filter(topoEntity -> topoEntity.getEntityType() ==
                        EntityType.VIRTUAL_VOLUME.getNumber())
                .filter(virtualVolume -> virtualVolume.getConnectedFromEntities().stream()
                        .anyMatch(topoEntity -> topoEntity.getEntityType() ==
                                EntityType.VIRTUAL_MACHINE.getNumber()))
                .collect(Collectors.toSet());
    }
}
