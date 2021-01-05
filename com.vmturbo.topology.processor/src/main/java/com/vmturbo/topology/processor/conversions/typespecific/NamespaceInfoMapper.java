package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.NamespaceInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.NamespaceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Namespace - i.e. {@link NamespaceInfo}.
 **/
public class NamespaceInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public TypeSpecificInfo
    mapEntityDtoToTypeSpecificInfo(@Nonnull final EntityDTOOrBuilder sdkEntity,
                                   @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasNamespaceData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        NamespaceData nsData = sdkEntity.getNamespaceData();
        NamespaceInfo.Builder infoBuilder = NamespaceInfo.newBuilder();
        if (nsData.hasAverageNodeCpuFrequency()) {
            infoBuilder.setAverageNodeCpuFrequency(nsData.getAverageNodeCpuFrequency());
        }

        return TypeSpecificInfo.newBuilder().setNamespace(infoBuilder)
                .build();
    }

}
