package com.vmturbo.api.component.external.api.mapper.aspect;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;

/**
 * Topology Extension data related to the shared data between disk array, logical pool,
 * storage controller.
 **/
public abstract class DiskCommonAspectMapper extends AbstractAspectMapper {

    protected void fillDiskTypeInfo(@Nonnull final STEntityAspectApiDTO aspect,
        @Nonnull final DiskTypeInfo diskTypeInfo) {

        if (diskTypeInfo.hasNumSsd() && diskTypeInfo.getNumSsd() > 0) {
            aspect.setSsdDiskCount(diskTypeInfo.getNumSsd());
        }
        if (diskTypeInfo.hasNum7200Disks() && diskTypeInfo.getNum7200Disks() > 0) {
            aspect.setRpm7200DiskCount(diskTypeInfo.getNum7200Disks());
        }
        if (diskTypeInfo.hasNum10KDisks() && diskTypeInfo.getNum10KDisks() > 0) {
            aspect.setRpm10KDiskCount(diskTypeInfo.getNum10KDisks());
        }
        if (diskTypeInfo.hasNum15KDisks() && diskTypeInfo.getNum15KDisks() > 0) {
            aspect.setRpm15KDiskCount(diskTypeInfo.getNum15KDisks());
        }
        if (diskTypeInfo.hasNumVSeriesDisks() && diskTypeInfo.getNumVSeriesDisks() > 0) {
            aspect.setvSeriesDiskCount(diskTypeInfo.getNumVSeriesDisks());
        }
    }
}
