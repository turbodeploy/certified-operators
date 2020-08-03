package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseServerTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseServerTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Database server tier- i.e. {@link DatabaseServerTierInfo}
 **/
public class DatabaseServerTierInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        final DatabaseServerTierData dbData = sdkEntity.getDatabaseServerTierData();
        return TypeSpecificInfo.newBuilder()
                .setDatabaseServerTier(DatabaseServerTierInfo.newBuilder()
                        .setFamily(dbData.getFamily())
                        .build())
                .build();
    }
}
