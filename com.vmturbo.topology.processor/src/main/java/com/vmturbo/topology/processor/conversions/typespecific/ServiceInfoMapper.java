package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ServiceInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Service - i.e. ServiceInfo.
 */
public class ServiceInfoMapper extends TypeSpecificInfoMapper {
    @Nonnull
    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull EntityDTOOrBuilder sdkEntity,
            @Nonnull Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasServiceData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ServiceData serviceData = sdkEntity.getServiceData();
        // Multiple platform types may map to one service type
        switch (serviceData.getServiceDataCase()) {
            case KUBERNETES_SERVICE_DATA:
                return TypeSpecificInfo.newBuilder()
                        .setService(ServiceInfo.newBuilder()
                            .setKubernetesServiceData(serviceData.getKubernetesServiceData()))
                        .build();
            default:
                return TypeSpecificInfo.getDefaultInstance();
        }
    }
}
