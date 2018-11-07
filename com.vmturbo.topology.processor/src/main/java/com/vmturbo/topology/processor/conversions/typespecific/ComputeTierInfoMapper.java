package com.vmturbo.topology.processor.conversions.typespecific;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a ComputeTier - i.e. {@link ComputeTierInfo}
 **/
public class ComputeTierInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(final EntityDTOOrBuilder sdkEntity) {
        if (!sdkEntity.hasComputeTierData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ComputeTierData ctData = sdkEntity.getComputeTierData();
        return TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                        .setFamily(ctData.getFamily())
                        .setDedicatedStorageNetworkState(ctData.getDedicatedStorageNetworkState())
                        .setNumCoupons(ctData.getNumCoupons())
                        .build())
                .build();
    }
}
