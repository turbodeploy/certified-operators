package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;

/**
 * Topology Extension data related to DiskArray.
 * TODO: this aspect is a subclass of DiskTypeAwareAspectMapper in Classic
 **/
public class DiskArrayAspectMapper implements IAspectMapper {
    @Override
    public EntityAspect mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
        STEntityAspectApiDTO aspect = new STEntityAspectApiDTO();
        if (!entity.getTypeSpecificInfo().hasStorage()) {
            return aspect;
        }
        final StorageInfo storageInfo = entity.getTypeSpecificInfo().getStorage();
        aspect.setDisplayName(entity.getDisplayName());
        final List<String> externalNameList = storageInfo.getExternalNameList();
        if (!externalNameList.isEmpty()) {
            aspect.setExternalNames(externalNameList);
        }
        // TODO: set the other aspect fields
        return aspect;
    }

    @Nonnull
    @Override
    public String getAspectName() {
        return "diskArrayAspect";
    }
}
