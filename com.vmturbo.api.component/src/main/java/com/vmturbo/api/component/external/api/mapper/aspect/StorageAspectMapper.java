package com.vmturbo.api.component.external.api.mapper.aspect;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.entityaspect.STEntityAspectApiDTO;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.RawCapacity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData.StoragePolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;

/**
 * Topology Extension data related to Storage.
 **/
public class StorageAspectMapper extends AbstractAspectMapper {
    @Override
    public STEntityAspectApiDTO mapEntityToAspect(@Nonnull final TopologyEntityDTO entity) {
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

        StorageType storageType = storageInfo.getStorageType();
        if (storageType == StorageType.VSAN) {
            // HCI Technology Type
            aspect.setTechnologyType(storageType.toString());
        }

        if (storageInfo.hasPolicy()) {
            StoragePolicy policy = storageInfo.getPolicy();
            aspect.setRedundancyMethod(policy.getRedundancy().toString());
            aspect.setFailuresToTolerate(policy.getFailuresToTolerate());
            aspect.setSpaceReservationPct(policy.getSpaceReservationPct());
        }

        if (storageInfo.hasRawCapacity()) {
            RawCapacity capacity = storageInfo.getRawCapacity();
            aspect.setRawCapacity(capacity.getCapacity());
            aspect.setRawFreespace(capacity.getFree());
            aspect.setRawUncommitted(capacity.getUncommitted());
        }

        return aspect;
    }

    @Nonnull
    @Override
    public AspectName getAspectName() {
        return AspectName.STORAGE;
    }
}
