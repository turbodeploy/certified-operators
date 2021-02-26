package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPlatformClusterInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ContainerPlatformClusterData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a ContainerPlatformCluster - i.e. ContainerPlatformClusterInfo.
 */
public class ContainerPlatformClusterInfoMapper extends TypeSpecificInfoMapper {
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(@Nonnull EntityDTOOrBuilder sdkEntity,
                                                           @Nonnull Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasContainerPlatformClusterData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        ContainerPlatformClusterData cntClusterData = sdkEntity.getContainerPlatformClusterData();
        ContainerPlatformClusterInfo.Builder infoBuilder = ContainerPlatformClusterInfo.newBuilder();
        if (cntClusterData.hasVcpuOvercommitment()) {
            infoBuilder.setVcpuOvercommitment(cntClusterData.getVcpuOvercommitment());
        }
        if (cntClusterData.hasVmemOvercommitment()) {
            infoBuilder.setVmemOvercommitment(cntClusterData.getVmemOvercommitment());
        }
        return TypeSpecificInfo.newBuilder().setContainerPlatformCluster(infoBuilder)
                .build();
    }
}
