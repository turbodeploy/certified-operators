package com.vmturbo.topology.processor.conversions.typespecific;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;

/**
 * Populate the {@link TypeSpecificInfo} unique to a Database - i.e. {@link DatabaseInfo}
 **/
public class DatabaseInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(final EntityDTOOrBuilder sdkEntity) {
        if (!sdkEntity.hasApplicationData() || !sdkEntity.getApplicationData().hasDbData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final ApplicationData appData = sdkEntity.getApplicationData();
        final DatabaseData dbData = appData.getDbData();
        return TypeSpecificInfo.newBuilder()
                .setDatabase(DatabaseInfo.newBuilder()
                        .setEdition(parseDbEdition(dbData.getEdition()))
                        .setEngine(parseDbEngine(dbData.getEngine()))
                        .build())
                .build();
    }
}
