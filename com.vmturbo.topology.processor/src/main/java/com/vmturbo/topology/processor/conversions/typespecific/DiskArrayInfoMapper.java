package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DiskArrayInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DiskArrayData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a disk array entity - i.e.
 **/
public class DiskArrayInfoMapper extends DiskTypeInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
        @Nonnull final EntityDTOOrBuilder sdkEntity,
        @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasDiskArrayData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final DiskTypeInfo.Builder dtInfo = DiskTypeInfo.newBuilder();
        final DiskArrayData daData = sdkEntity.getDiskArrayData();
        if (daData.hasIopsComputeData()) {
            setupIopsComputeData(sdkEntity.getDisplayName(),
                daData.getIopsComputeData().getIopsItemsList(), dtInfo);
        }
        return TypeSpecificInfo.newBuilder()
            .setDiskArray(DiskArrayInfo.newBuilder()
                .setDiskTypeInfo(dtInfo))
            .build();
    }

}
