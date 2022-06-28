package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to an ApplicationService - i.e. {@link ApplicationServiceInfo}.
 **/
public class ApplicationServiceInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasApplicationServiceData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ApplicationServiceInfo appSvcInfo = getApplicationServiceInfo(
                sdkEntity.getApplicationServiceData());
        return TypeSpecificInfo.newBuilder().setApplicationService(appSvcInfo).build();
    }

    // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
    //  This method can be private once it's no longer used by ApplicationInfoMapper
    protected ApplicationServiceInfo getApplicationServiceInfo(ApplicationServiceData appSvcData) {
        Builder appSvcInfoBuilder = TypeSpecificInfo.ApplicationServiceInfo.newBuilder();
        if (appSvcData.hasPlatform()) {
            appSvcInfoBuilder.setPlatform(
                    TypeSpecificInfo.ApplicationServiceInfo.Platform.valueOf(
                            appSvcData.getPlatform().toString()));
        }
        if (appSvcData.hasTier()) {
            appSvcInfoBuilder.setTier(
                    TypeSpecificInfo.ApplicationServiceInfo.Tier.valueOf(
                            appSvcData.getTier().toString()));
        }
        if (appSvcData.hasMaxInstanceCount()) {
            appSvcInfoBuilder.setMaxInstanceCount(appSvcData.getMaxInstanceCount());
        }
        if (appSvcData.hasCurrentInstanceCount()) {
            appSvcInfoBuilder.setCurrentInstanceCount(appSvcData.getCurrentInstanceCount());
        }
        if (appSvcData.hasAppCount()) {
            appSvcInfoBuilder.setAppCount(appSvcData.getAppCount());
        }
        if (appSvcData.hasZoneRedundant()) {
            appSvcInfoBuilder.setZoneRedundant(appSvcData.getZoneRedundant());
        }
        return appSvcInfoBuilder.build();
    }
}
