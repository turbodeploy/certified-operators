package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a BusinessAccount - i.e. {@link BusinessAccountInfo}
 **/
public class BusinessAccountInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasBusinessAccountData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final BusinessAccountData baData = sdkEntity.getBusinessAccountData();
        BusinessAccountInfo.Builder baInfoBuilder = BusinessAccountInfo.newBuilder()
            .setHasAssociatedTarget(baData.getDataDiscovered());
        if (baData.hasAccountId()) {
            baInfoBuilder.setAccountId(baData.getAccountId());
        }
        baInfoBuilder.addAllPricingIdentifiers(baData.getPricingIdentifiersList());
        return TypeSpecificInfo.newBuilder()
            .setBusinessAccount(baInfoBuilder.build())
            .build();
    }
}