package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPodInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerPodData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a ContainerPod - i.e. ContainerPodInfo.
 */
public class ContainerPodInfoMapper extends TypeSpecificInfoMapper {
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasContainerPodData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        ContainerPodData containerPodData = sdkEntity.getContainerPodData();
        ContainerPodInfo.Builder infoBuilder = ContainerPodInfo.newBuilder();
        if (containerPodData.hasHostingNodeCpuFrequency()) {
            infoBuilder.setHostingNodeCpuFrequency(containerPodData.getHostingNodeCpuFrequency());
        }
        return TypeSpecificInfo.newBuilder().setContainerPod(infoBuilder).build();
    }
}
