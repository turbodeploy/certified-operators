package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.LogicalPoolInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LogicalPoolData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a logical pool entity - i.e.
 **/
public class LogicalPoolInfoMapper extends DiskTypeInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
        @Nonnull final EntityDTOOrBuilder sdkEntity,
        @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasLogicalPoolData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final DiskTypeInfo.Builder dtInfo = DiskTypeInfo.newBuilder();
        final LogicalPoolData lpData = sdkEntity.getLogicalPoolData();
        if (lpData.hasIopsComputeData()) {
            setupIopsComputeData(sdkEntity.getDisplayName(),
                lpData.getIopsComputeData().getIopsItemsList(), dtInfo);
        }
        return TypeSpecificInfo.newBuilder()
            .setLogicalPool(LogicalPoolInfo.newBuilder()
                .setDiskTypeInfo(dtInfo))
            .build();
    }

}
