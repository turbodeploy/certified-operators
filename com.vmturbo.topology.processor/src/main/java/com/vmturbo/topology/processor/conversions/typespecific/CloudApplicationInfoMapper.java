package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudApplicationInfo.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Cloud PaaS application -
 * i.e. {@link CloudApplicationInfo}.
 **/
public class CloudApplicationInfoMapper extends TypeSpecificInfoMapper {
    /**
     * The name the number of deployment slots used by the app service.
     */
    private static final String APP_SERVICE_DEPLOYMENT_SLOTS = "Deployment Slots";

    private static final String APP_SERVICE_HYBRID_CONNECTIONS = "Hybrid Connections";


    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        final CloudApplicationInfo appSvcInfo =
                getCloudApplicationInfo(sdkEntity.getEntityPropertiesList());
        return TypeSpecificInfo.newBuilder().setCloudApplication(appSvcInfo).build();
    }

    protected CloudApplicationInfo getCloudApplicationInfo(
            List<EntityProperty> properties) {
        Builder builder = CloudApplicationInfo.newBuilder();

        for (EntityProperty property : properties) {
            String name = property.getName();
            if (APP_SERVICE_DEPLOYMENT_SLOTS.equals(name)) {
                builder.setDeploymentSlotCount(Integer.parseInt(property.getValue()));
            } else if (APP_SERVICE_HYBRID_CONNECTIONS.equals(name)) {
                builder.setHybridConnectionCount(Integer.parseInt(property.getValue()));
            }
        }

        return builder.build();
    }
}
