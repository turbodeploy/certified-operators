package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageControllerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageControllerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a storage controller entity - i.e.
 **/
public class StorageControllerInfoMapper extends DiskTypeInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
        @Nonnull final EntityDTOOrBuilder sdkEntity,
        @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasStorageControllerData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final DiskTypeInfo.Builder dtInfo = DiskTypeInfo.newBuilder();
        final StorageControllerData scData = sdkEntity.getStorageControllerData();
        if (scData.hasIopsComputeData()) {
            setupIopsComputeData(sdkEntity.getDisplayName(),
                scData.getIopsComputeData().getIopsItemsList(), dtInfo);
        }
        return TypeSpecificInfo.newBuilder()
            .setStorageController(StorageControllerInfo.newBuilder()
                .setDiskTypeInfo(dtInfo))
            .build();
    }

}
