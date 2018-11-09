package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.protobuf.ProtocolStringList;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PMEntityAspectApiDTO;
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
        final List<String> externalNameList = storageInfo.getExternalNameList();
        if (!externalNameList.isEmpty()) {
            aspect.setExternalNames(externalNameList);
        }
        // TODO: populate the other fields
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "storageAspect";
    }
}
