package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Container - i.e. ContainerInfo.
 */
public class ContainerInfoMapper extends TypeSpecificInfoMapper {
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasContainerData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        ContainerData containerData = sdkEntity.getContainerData();
        ContainerInfo.Builder infoBuilder = ContainerInfo.newBuilder();
        if (containerData.hasHasCpuLimit()) {
            infoBuilder.setHasCpuLimit(containerData.getHasCpuLimit());
        }
        if (containerData.hasHasMemLimit()) {
            infoBuilder.setHasMemLimit(containerData.getHasMemLimit());
        }
        return TypeSpecificInfo.newBuilder().setContainer(infoBuilder).build();
    }
}
