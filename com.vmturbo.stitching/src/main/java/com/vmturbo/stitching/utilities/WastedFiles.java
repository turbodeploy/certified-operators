package com.vmturbo.stitching.utilities;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Util class for wasted files.
 */
public class WastedFiles {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Private constructor.
     */
    private WastedFiles() {
    }

    /**
     * Find the VirtualVolume representing wasted files for a given Storage.  The wasted files
     * VirtualVolume can be identified by the fact that it has no connected VirtualMachines
     *
     * @param storage the Storage whose wasted files VirtualVolume we want.
     * @return Optional containing the wasted files VirtualVolume or else Optional.empty if no such
     * VirtualVolume exists.
     */
    public static Optional<TopologyEntity> getWastedFilesVirtualVolume(TopologyEntity storage) {
        List<TopologyEntity> wastedFilesVolumes =
                storage.getInboundAssociatedEntities().stream()
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                        .filter(virtualVolume -> virtualVolume.getInboundAssociatedEntities().stream()
                                .map(TopologyEntity::getEntityType)
                                .noneMatch(type -> type == EntityType.VIRTUAL_MACHINE_VALUE))
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
}
