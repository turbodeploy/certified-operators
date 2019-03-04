package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;

/**
 * Topology Extension data related to Storage.
 **/
public class StorageAspectMapper implements IAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        aspect.setDisplayName(entity.getDisplayName());
        if (!entity.getTypeSpecificInfo().hasStorage()) {
            return null;
        }
        final StorageInfo storageInfo = entity.getTypeSpecificInfo().getStorage();
        // external names contains duplicate, we need to filter them out
        final Set<String> externalNames = storageInfo.getExternalNameList().stream()
            .collect(Collectors.toSet());
        if (!externalNames.isEmpty()) {
            aspect.setExternalNames(externalNames.stream().collect(Collectors.toList()));
        }

        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "storageAspect";
    }
}
