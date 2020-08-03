package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Database tier - i.e. {@link DatabaseTierInfo}
 **/
public class DatabaseTierInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        final DatabaseTierData dbData = sdkEntity.getDatabaseTierData();
        return TypeSpecificInfo.newBuilder()
                .setDatabaseTier(DatabaseTierInfo.newBuilder()
                        .setFamily(dbData.getFamily())
                        .setEdition(dbData.getEdition())
                        .build())
                .build();
    }
}
