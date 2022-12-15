package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerSpecInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerSpecData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a ContainerSpec - i.e. ContainerSpecInfo.
 */
public class ContainerSpecInfoMapper extends TypeSpecificInfoMapper {
    @Nonnull
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasContainerSpecData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        ContainerSpecData containerSpecData = sdkEntity.getContainerSpecData();
        if (!containerSpecData.hasCpuThrottlingType()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        ContainerSpecInfo.Builder containerSpecInfoBuilder = ContainerSpecInfo.newBuilder()
                .setCpuThrottlingType(containerSpecData.getCpuThrottlingType());
        return TypeSpecificInfo.newBuilder()
                .setContainerSpec(containerSpecInfoBuilder)
                .build();
    }
}
