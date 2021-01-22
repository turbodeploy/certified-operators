/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.Nullable;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualVolumeEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * {@link VirtualVolumeEntityAspectMapper} creates aspect which is based on the virtual volume
 * entity itself. It does not use entities of other types to build aspect instance.
 */
public class VirtualVolumeEntityAspectMapper extends AbstractAspectMapper {
    @Nullable
    @Override
    public <T extends EntityAspect> T mapEntityToAspect(@Nonnull TopologyEntityDTO entity)
                    throws InterruptedException, ConversionException {
        if (hasVirtualVolumeSpecificInfo(entity)) {
            return null;
        }
        final VirtualVolumeInfo vvInfo = entity.getTypeSpecificInfo().getVirtualVolume();
        final Set<String> files =
                        vvInfo.getFilesList().stream().map(VirtualVolumeFileDescriptor::getPath)
                                        .collect(Collectors.toSet());
        final VirtualVolumeEntityAspectApiDTO result = new VirtualVolumeEntityAspectApiDTO();
        result.setFiles(new ArrayList<>(files));
        @SuppressWarnings("unchecked")
        final T castedResult = (T)result;
        return castedResult;
    }

    private static boolean hasVirtualVolumeSpecificInfo(
                    @Nonnull TopologyEntityDTOOrBuilder entity) {
        return entity.getEntityType() != EntityType.VIRTUAL_VOLUME_VALUE || !entity
                        .hasTypeSpecificInfo() || !entity.getTypeSpecificInfo().hasVirtualVolume();
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.VIRTUAL_VOLUME_ENTITY;
    }
}
