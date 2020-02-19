package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Storage entity - i.e. {@link StorageInfo}
 **/
public class StorageInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasStorageData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final StorageData storageData = sdkEntity.getStorageData();
        boolean isLocal = Boolean.valueOf(entityPropertyMap.get("local"));
        // remove local from entityPropertyMap to prevent duplication of data on typeSpecificInfo
        entityPropertyMap.remove("local");
        return TypeSpecificInfo.newBuilder()
                .setStorage(StorageInfo.newBuilder()
                        .setStorageType(storageData.getStorageType())
                        .setIsLocal(isLocal)
                        .addAllExternalName(storageData.getExternalNameList()))
                .build();
    }
}
